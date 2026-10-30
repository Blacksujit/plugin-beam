package io.kestra.plugin.beam.config;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FlinkRunnerConfigTest {

    @Test
    void shouldMapExecutionModeToFlinkMaster() {
        var config = FlinkRunnerConfig.builder()
            .executionMode("[local]")
            .build();

        assertThat(config.toPipelineOptions(), hasEntry("flink_master", "[local]"));
    }

    @Test
    void shouldMapFlinkRestUrlToFlinkMaster() {
        var config = FlinkRunnerConfig.builder()
            .flinkRestUrl("http://localhost:8082")
            .build();

        assertThat(config.toPipelineOptions(), hasEntry("flink_master", "http://localhost:8082"));
    }

    @Test
    void shouldApplyExecutionModeAsFlinkMasterWithJavaSdk() {
        var config = FlinkRunnerConfig.builder()
            .executionMode("[local]")
            .parallelism(2)
            .build();

        assertThat(config.toJavaPipelineOptions(), not(hasKey("flink_master")));
        assertThat(config.toJavaPipelineOptions(), hasEntry("parallelism", 2));

        var options = PipelineOptionsFactory.fromArgs("--parallelism=2").withValidation().create();
        config.applyTo(options);

        assertThat(options.as(FlinkPipelineOptions.class).getFlinkMaster(), is("[local]"));
    }

    @Test
    void shouldRejectBothExecutionModeAndFlinkRestUrl() {
        var config = FlinkRunnerConfig.builder()
            .executionMode("[local]")
            .flinkRestUrl("http://localhost:8082")
            .build();

        assertThrows(IllegalArgumentException.class, config::toPipelineOptions);
    }

    @Test
    void shouldUseJavaOptionNamesWithJavaSdk() {
        var config = FlinkRunnerConfig.builder()
            .parallelism(2)
            .savepointDir("/tmp/sp")
            .stateBackend("rocksdb")
            .stateBackendStoragePath("file:///tmp/state")
            .jarPath("/tmp/job.jar")
            .build();

        var args = config.toJavaPipelineOptions().entrySet().stream()
            .map(e -> "--" + e.getKey() + "=" + e.getValue())
            .toArray(String[]::new);
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().create().as(FlinkPipelineOptions.class);

        assertThat(options.getParallelism(), is(2));
        assertThat(options.getSavepointPath(), is("/tmp/sp"));
        assertThat(options.getStateBackend(), is("rocksdb"));
        assertThat(options.getStateBackendStoragePath(), is("file:///tmp/state"));
        assertThat(options.getFilesToStage(), contains("/tmp/job.jar"));
    }

    @Test
    void shouldKeepPythonOptionNamesWithPythonSdk() {
        var config = FlinkRunnerConfig.builder()
            .parallelism(2)
            .savepointDir("/tmp/sp")
            .stateBackend("rocksdb")
            .stateBackendStoragePath("file:///tmp/state")
            .jarPath("/tmp/job.jar")
            .build();

        assertThat(config.toPipelineOptions(), is(Map.<String, Object>of(
            "parallelism", 2,
            "savepoint_path", "/tmp/sp",
            "state_backend", "rocksdb",
            "state_backend_storage_path", "file:///tmp/state",
            "files_to_stage", "/tmp/job.jar"
        )));
    }
}
