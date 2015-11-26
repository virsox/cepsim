package ca.uwo.eng.sel.cepsim.bench;

import ca.uwo.eng.sel.cepsim.example.ResourceConsumptionTest;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class CepSimBenchmark {

    @Param({"10", "100", "200", "300", "400", "500", "600", "700", "800", "900", "1000"})
    public int numberOfVms;

    @Param({"10"})
    public int queriesPerVm;

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Timeout(time = 900)
    @Fork(10)
    public void testMethod() throws InterruptedException {
        new ResourceConsumptionTest().simulate(numberOfVms, queriesPerVm);
    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CepSimBenchmark.class.getSimpleName())
                .forks(1)
                .addProfiler(GCProfiler.class)
                //.warmupIterations(0)
                //.measurementIterations(1)
                .build();

        new Runner(opt).run();
    }

}
