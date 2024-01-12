package com.databricks.jdbc.local;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.opentelemetry.semconv.ResourceAttributes.SERVICE_NAME;

public class OtelTester {

    static final class ExampleConfiguration {

        /**
         * Initialize OpenTelemetry.
         *
         * @return a ready-to-use {@link OpenTelemetry} instance.
         */
        static OpenTelemetry initOpenTelemetry() {
            // Include required service.name resource attribute on all spans and metrics
            Resource resource = Resource.getDefault().merge(Resource.builder()
                    .put(SERVICE_NAME, "OtlpExporterExample").build());
            OpenTelemetrySdk openTelemetrySdk = OpenTelemetrySdk.builder()
                    .setTracerProvider(SdkTracerProvider.builder()
                            .setResource(resource)
                            .addSpanProcessor(BatchSpanProcessor.builder(
                                    OtlpGrpcSpanExporter.builder()
                                            .setTimeout(2, TimeUnit.SECONDS).build())
                                    .setScheduleDelay(100, TimeUnit.MILLISECONDS).build())
                            .build())
                    .setMeterProvider(SdkMeterProvider.builder()
                            .setResource(resource)
                            .registerMetricReader(PeriodicMetricReader.builder(
                                    OtlpGrpcMetricExporter.getDefault())
                                    .setInterval(Duration.ofMillis(1000)).build())
                            .build())
                    .setLoggerProvider(SdkLoggerProvider.builder()
                            .setResource(resource)
                            .addLogRecordProcessor(BatchLogRecordProcessor.builder(
                                    OtlpGrpcLogRecordExporter.getDefault())
                                    .build())
                            .build())
                    .buildAndRegisterGlobal();
            Runtime.getRuntime().addShutdownHook(new Thread(openTelemetrySdk::close));


            io.opentelemetry.instrumentation.log4j.appender.v2_17.OpenTelemetryAppender.install(
                    openTelemetrySdk);

            return openTelemetrySdk;

//            return GlobalOpenTelemetry.get();
        }
    }


    private static final Logger LOGGER = LoggerFactory.getLogger(OtelTester.class);
    static final AttributeKey<String> ATTR_METHOD = AttributeKey.stringKey("method");

    static final Random random = new Random();

    public static void main(String[] args) throws InterruptedException {
        OpenTelemetry openTelemetry = ExampleConfiguration.initOpenTelemetry();


        final Tracer tracer = openTelemetry.getTracer(ExampleConfiguration.class.getName());
        Meter meter = openTelemetry.getMeter(ExampleConfiguration.class.getName());
        final LongHistogram doWorkHistogram = meter.histogramBuilder("do-work").ofLongs().build();

        int sleepTime = random.nextInt(200);
        LOGGER.info("Sleeping {}ms", sleepTime);

        Span span = tracer.spanBuilder("doWork").startSpan();
        try (Scope ignored = span.makeCurrent()) {
            Thread.sleep(sleepTime);
            LOGGER.info("A sample log message!");
        } finally {
            span.end();
        }
        LOGGER.info("Work is done!");

        doWorkHistogram.record(sleepTime, Attributes.of(ATTR_METHOD, "ping"));
    }
}
