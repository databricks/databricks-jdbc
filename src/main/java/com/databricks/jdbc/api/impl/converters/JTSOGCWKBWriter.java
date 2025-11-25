package com.databricks.jdbc.api.impl.converters;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.EnumSet;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.Ordinate;

// Implements a WKB writer for JTS geometries that is OGC compliant.
public class JTSOGCWKBWriter {
  // Constructs a write using the enum set for the output coordinates and the provided byte order
  // (endianness).
  public JTSOGCWKBWriter(EnumSet<Ordinate> outputOrdinates, ByteOrder byteOrder) {
    this.outputOrdinates = outputOrdinates;
    this.byteOrder = byteOrder;
    this.outputDimension = 2;
    if (hasZ()) ++this.outputDimension;
    if (hasM()) ++this.outputDimension;
  }

  // Returns the WKB description of the input JTS geometry.
  public byte[] write(Geometry geom) {
    byte[] wkb = new byte[numBytes(geom)];
    ByteBuffer buffer = ByteBuffer.wrap(wkb).order(byteOrder);
    write(buffer, /* pos= */ 0, geom);
    return wkb;
  }

  // Helper method to compute the number of bytes for the WKB description of the input geometry.
  // It performs dispatching to the concrete classes implementing the `Geometry` abstract class.
  private int numBytes(Geometry geom) {
    if (geom instanceof Point) {
      return numBytes((Point) geom);
    } else if (geom instanceof LineString) {
      return numBytes((LineString) geom);
    } else if (geom instanceof Polygon) {
      return numBytes((Polygon) geom);
    } else if (geom instanceof GeometryCollection) {
      // This accounts for multipoints, multilinestring, and multipolygons as well.
      return numBytes((GeometryCollection) geom);
    } else {
      throw new IllegalArgumentException("numBytes: Unknown or unsupported type");
    }
  }

  // Number of bytes for a byte.
  private static final int SIZE_OF_BYTE = Byte.BYTES;
  // Number of bytes for integers.
  private static final int SIZE_OF_INT = Integer.BYTES;
  // Number of bytes for doubles.
  private static final int SIZE_OF_DOUBLE = Double.BYTES;

  // Returns the number of bytes required for the WKB description of the input point.
  private int numBytes(Point point) {
    // We need:
    // - 1 byte for the endianness.
    // - 4 bytes for encoding the type.
    // - 8 bytes per coordinate.
    return (SIZE_OF_BYTE + SIZE_OF_INT) + outputDimension * SIZE_OF_DOUBLE;
  }

  // Returns the number of bytes required for the WKB description of the input linestring.
  private int numBytes(LineString lineString) {
    // We need:
    // - 1 byte for the endianness.
    // - 4 bytes for encoding the type.
    // - 4 bytes for the number of points in the linestring.
    // - 8 bytes per coordinate, per point.
    return (SIZE_OF_BYTE + 2 * SIZE_OF_INT)
        + lineString.getNumPoints() * outputDimension * SIZE_OF_DOUBLE;
  }

  // Returns the number of bytes required for the WKB description of the input polygon.
  private int numBytes(Polygon polygon) {
    // We need:
    // - 1 byte for the endianness.
    // - 4 bytes for encoding the type.
    // - 4 bytes for the number of rings in the polygon.
    // - 4 bytes for the number of points per ring.
    // - 8 bytes per coordinate, per point.
    return (SIZE_OF_BYTE + 2 * SIZE_OF_INT)
        + polygon.getNumPoints() * outputDimension * SIZE_OF_DOUBLE
        + numRings(polygon) * SIZE_OF_INT;
  }

  // Returns the number of bytes required for the WKB description of the input collection (in JTS
  // this accounts for multipoints, multilinestrings, and multipolygons).
  private int numBytes(GeometryCollection collection) {
    // We need:
    // - 1 byte for the endianness.
    // - 4 bytes for encoding the type.
    // - 4 bytes for the number of geometries in the collection.
    // - The number of bytes to represent each geometry in the collection.
    int numBytes = (SIZE_OF_BYTE + 2 * SIZE_OF_INT);
    for (int i = 0; i < collection.getNumGeometries(); ++i) {
      numBytes += numBytes(collection.getGeometryN(i));
    }
    return numBytes;
  }

  // Returns the number of rings of the input polygon.
  private int numRings(Polygon polygon) {
    return polygon.isEmpty() ? 0 : (polygon.getNumInteriorRing() + 1);
  }

  // OGC type values for the various geometry types. The values below are for 2D geometries.
  private static final int POINT_TYPE = 1;
  private static final int LINESTRING_TYPE = 2;
  private static final int POLYGON_TYPE = 3;
  private static final int MULTIPOINT_TYPE = 4;
  private static final int MULTILINESTRING_TYPE = 5;
  private static final int MULTIPOLYGON_TYPE = 6;
  private static final int GEOMETRYCOLLECTION_TYPE = 7;

  // Writes the WKB description of the input geometry starting at position `pos` in the provided
  // buffer. Returns the position in the buffer after the written bytes.
  public int write(ByteBuffer buffer, int pos, Geometry geom) {
    if (geom instanceof Point) {
      return write(buffer, pos, (Point) geom);
    } else if (geom instanceof LineString) {
      return write(buffer, pos, (LineString) geom);
    } else if (geom instanceof Polygon) {
      return write(buffer, pos, (Polygon) geom);
    } else if (geom instanceof GeometryCollection) {
      // This accounts for multipoints, multilinestring, and multipolygons as well.
      return write(buffer, pos, (GeometryCollection) geom);
    } else {
      throw new IllegalArgumentException("write: Unknown or unsupported type");
    }
  }

  // Writes the WKB description of the input point starting at position `pos` in the provided
  // buffer. Returns the position in the buffer after the written bytes.
  private int write(ByteBuffer buffer, int pos, Point point) {
    // Write the endianness.
    buffer.put(pos, (byte) (isLittleEndian() ? 1 : 0));
    pos += SIZE_OF_BYTE;
    // Write the type.
    buffer.putInt(pos, fromBaseType(POINT_TYPE));
    // Write the point coordinates and return the position after the written bytes.
    return writeCoords(buffer, pos + SIZE_OF_INT, point.getCoordinate());
  }

  // Writes the WKB description of the input linestring starting at position `pos` in the provided
  // buffer. Returns the position in the buffer after the written bytes.
  private int write(ByteBuffer buffer, int pos, LineString lineString) {
    // Write the endianness.
    buffer.put(pos, (byte) (isLittleEndian() ? 1 : 0));
    pos += SIZE_OF_BYTE;
    // Write the type.
    buffer.putInt(pos, fromBaseType(LINESTRING_TYPE));
    pos += SIZE_OF_INT;
    // Write the number of points.
    buffer.putInt(pos, lineString.getNumPoints());
    pos += SIZE_OF_INT;
    // Write the point coordinates.
    for (int i = 0; i < lineString.getNumPoints(); ++i) {
      pos = writeCoords(buffer, pos, lineString.getCoordinateN(i));
    }
    // Return the position after the written bytes.
    return pos;
  }

  // Writes the WKB description of the input polygon starting at position `pos` in the provided
  // buffer. Returns the position in the buffer after the written bytes.
  private int write(ByteBuffer buffer, int pos, Polygon polygon) {
    // Write the endianness.
    buffer.put(pos, (byte) (isLittleEndian() ? 1 : 0));
    pos += SIZE_OF_BYTE;
    // Write the type.
    buffer.putInt(pos, fromBaseType(POLYGON_TYPE));
    pos += SIZE_OF_INT;
    int numRings = numRings(polygon);
    // Write the number of rings.
    buffer.putInt(pos, numRings);
    pos += SIZE_OF_INT;
    if (numRings > 0) {
      LinearRing outer = polygon.getExteriorRing();
      // Write number of points in the outer ring.
      buffer.putInt(pos, outer.getNumPoints());
      pos += SIZE_OF_INT;
      // Write the point coordinates in the outer ring.
      for (int i = 0; i < outer.getNumPoints(); ++i) {
        pos = writeCoords(buffer, pos, outer.getCoordinateN(i));
      }
    }
    for (int i = 0; i < polygon.getNumInteriorRing(); ++i) {
      LinearRing inner = polygon.getInteriorRingN(i);
      // Write number of points in the i-th inner ring.
      buffer.putInt(pos, inner.getNumPoints());
      pos += SIZE_OF_INT;
      // Write the point coordinates in the i-th inner ring.
      for (int j = 0; j < inner.getNumPoints(); ++j) {
        pos = writeCoords(buffer, pos, inner.getCoordinateN(j));
      }
    }
    return pos;
  }

  // Writes the WKB description of the input collection (includes multipoints, multilinestrings,
  // and multipolygons) starting at position `pos` in the provided buffer. Returns the position in
  // the buffer after the written bytes.
  private int write(ByteBuffer buffer, int pos, GeometryCollection collection) {
    // Write the endianness.
    buffer.put(pos, (byte) (isLittleEndian() ? 1 : 0));
    pos += SIZE_OF_BYTE;
    // Write the type.
    if (collection instanceof MultiPoint) {
      buffer.putInt(pos, fromBaseType(MULTIPOINT_TYPE));
    } else if (collection instanceof MultiLineString) {
      buffer.putInt(pos, fromBaseType(MULTILINESTRING_TYPE));
    } else if (collection instanceof MultiPolygon) {
      buffer.putInt(pos, fromBaseType(MULTIPOLYGON_TYPE));
    } else {
      buffer.putInt(pos, fromBaseType(GEOMETRYCOLLECTION_TYPE));
    }
    pos += SIZE_OF_INT;
    // Write the number of geometries.
    buffer.putInt(pos, collection.getNumGeometries());
    pos += SIZE_OF_INT;
    for (int i = 0; i < collection.getNumGeometries(); ++i) {
      // Get the WKB of the i-th geometry.
      pos = write(buffer, pos, collection.getGeometryN(i));
    }
    return pos;
  }

  // Write the coordinates to the provided byte buffer. Returns the next position in the buffer,
  // after the written bytes.
  private int writeCoords(ByteBuffer buffer, int pos, Coordinate coords) {
    if (coords == null) {
      // We have an empty point. We need to output `Coordinate.NULL_ORDINATE` for all point
      // coordinates in this case.
      buffer.putDouble(pos, Coordinate.NULL_ORDINATE);
      pos += SIZE_OF_DOUBLE;
      buffer.putDouble(pos, Coordinate.NULL_ORDINATE);
      if (hasZ()) {
        pos += SIZE_OF_DOUBLE;
        buffer.putDouble(pos, Coordinate.NULL_ORDINATE);
      }
      if (hasM()) {
        pos += SIZE_OF_DOUBLE;
        buffer.putDouble(pos, Coordinate.NULL_ORDINATE);
      }
      return pos + SIZE_OF_DOUBLE;
    }
    buffer.putDouble(pos, coords.getX());
    pos += SIZE_OF_DOUBLE;
    buffer.putDouble(pos, coords.getY());
    if (hasZ()) {
      pos += SIZE_OF_DOUBLE;
      buffer.putDouble(pos, coords.getZ());
    }
    if (hasM()) {
      pos += SIZE_OF_DOUBLE;
      buffer.putDouble(pos, coords.getM());
    }
    return pos + SIZE_OF_DOUBLE;
  }

  // Returns `true` iff the byte order specified at construction is little endian.
  private boolean isLittleEndian() {
    return byteOrder == ByteOrder.LITTLE_ENDIAN;
  }

  // Returns `true` iff the ordinates specified at construction include the Z ordinate.
  private boolean hasZ() {
    return outputOrdinates.contains(Ordinate.Z);
  }

  // Returns `true` iff the ordinates specified at construction include the M ordinate.
  private boolean hasM() {
    return outputOrdinates.contains(Ordinate.M);
  }

  // Offsets (with respect to the 2D OGC type value) for the OGC type valuefor geometries with Z
  // and/or M coordinates.
  private static final int Z_TYPE_OFFSET = 1000;
  private static final int M_TYPE_OFFSET = 2000;
  private static final int ZM_TYPE_OFFSET = 3000;

  // Given the base OGC type value for a geometry type, returns the OGC type value that takes the
  // dimension of the geometry into account.
  private int fromBaseType(int baseType) {
    boolean hasZ = hasZ();
    boolean hasM = hasM();
    if (hasZ && hasM) return baseType + ZM_TYPE_OFFSET;
    if (hasZ) return baseType + Z_TYPE_OFFSET;
    if (hasM) return baseType + M_TYPE_OFFSET;
    return baseType;
  }

  // The ordinates we want to output when we write the WKB description of a geometry using this
  // writer.
  private EnumSet<Ordinate> outputOrdinates;
  // The byte order (endianness) of the WKB description of a geometry written using this writer.
  private ByteOrder byteOrder;
  // The number of dimensions we write in the WKB description of a geometry written using this
  // writer. This is a derivative quantity computed from `outputOrdinates` and is stored for
  // efficiency.
  private int outputDimension;
}
