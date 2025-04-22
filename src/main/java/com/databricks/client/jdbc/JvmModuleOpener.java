package com.databricks.client.jdbc;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

// This class must be in a package that gets loaded before any Arrow classes
public class JvmModuleOpener extends ClassLoader {
    // This static initializer will run when the class is loaded
    static {
        openJavaNioModule();
    }

    private static void openJavaNioModule() {
        try {
            if (Runtime.version().feature() >= 16) {
                // Get the java.base module
                Module javaBaseModule = Object.class.getModule();

                // Get all classloaders in the hierarchy to open to all modules
                ClassLoader currentClassLoader = JvmModuleOpener.class.getClassLoader();

                // Start with the current classloader's unnamed module
                Module unnamedModule = currentClassLoader.getUnnamedModule();

                try {
                    // Try to use the less aggressive implAddOpens first
                    Method implAddOpens = Module.class.getDeclaredMethod("implAddOpens", String.class, Module.class);
                    implAddOpens.setAccessible(true);
                    implAddOpens.invoke(javaBaseModule, "java.nio", unnamedModule);

                    // Also try to open to ALL-UNNAMED directly if available
                    try {
                        // Sometimes we need to open to all unnamed modules (not just our classloader's)
                        Field EVERYONE_MODULE = Module.class.getDeclaredField("EVERYONE_MODULE");
                        EVERYONE_MODULE.setAccessible(true);
                        Module everyoneModule = (Module) EVERYONE_MODULE.get(null);
                        implAddOpens.invoke(javaBaseModule, "java.nio", everyoneModule);
                    } catch (Exception ignored) {
                        // If this fails, the regular implAddOpens might still work
                    }

                    // Verify that the module has been opened successfully
                    try {
                        // Create a simple test that tries to access the field
                        // If this works, our module opening was successful
                        Field addressField = java.nio.Buffer.class.getDeclaredField("address");
                        addressField.setAccessible(true);
                        // No need to actually access it, just checking that it doesn't throw
                        System.out.println("Successfully opened java.nio module");
                    } catch (Exception e) {
                        throw new RuntimeException("Module opening verification failed", e);
                    }
                } catch (Exception e) {
                    // If the direct approach failed, try an alternative method
                    System.err.println("First module opening attempt failed: " + e.getMessage());

                    // Try a more direct approach with sun.misc.Unsafe
                    try {
                        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
                        Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
                        theUnsafeField.setAccessible(true);
                        Object unsafe = theUnsafeField.get(null);

                        // Use Unsafe to directly modify module permissions
                        // This is very aggressive but may work when other approaches fail
                        try {
                            // Get the addExports/addOpens methods from Unsafe
                            Method addOpensMethod = unsafeClass.getMethod("addOpens",
                                    String.class, String.class, Module.class);

                            // Use them to open java.nio to the unnamed module
                            addOpensMethod.invoke(unsafe, "java.base", "java.nio", unnamedModule);
                            System.out.println("Successfully opened java.nio module using Unsafe");
                        } catch (NoSuchMethodException nsme) {
                            // Some versions of Unsafe don't have this method
                            System.err.println("Unsafe doesn't have addOpens method");
                        }
                    } catch (Exception ex) {
                        System.err.println("Alternative module opening attempt failed: " + ex.getMessage());
                    }
                }
            }
        } catch (Throwable t) {
            // Catch Throwable to intercept any errors, we don't want to prevent driver loading
            System.err.println("Failed to open java.nio module: " + t.getMessage());
            System.err.println("You may need to add --add-opens=java.base/java.nio=ALL-UNNAMED to your JVM arguments");
        }
    }

    public static void ensureInitialized() {}
}