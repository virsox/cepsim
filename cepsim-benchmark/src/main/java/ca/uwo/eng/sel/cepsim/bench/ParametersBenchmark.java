package ca.uwo.eng.sel.cepsim.bench;

import ca.uwo.eng.sel.cepsim.example.CepSimAvgWindow;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

import static ca.uwo.eng.sel.cepsim.example.CepSimAvgWindow.AllocStrategyEnum.*;
import static ca.uwo.eng.sel.cepsim.example.CepSimAvgWindow.SchedStrategyEnum.*;

@State(Scope.Benchmark)
public class ParametersBenchmark {

    @Param({"1.0"})
    public double simInterval;

    @Param({"1", "10", "100"})
    public int iterations;

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(10)
    public void testMethod() throws InterruptedException {
        new CepSimAvgWindow().simulate(DYNAMIC, UNIFORM, simInterval, iterations);
        //new CepSimAvgWindowNetwork().simulate(simInterval, iterations);
    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ParametersBenchmark.class.getSimpleName())
                .forks(1)
                //.warmupIterations(0)
                //.measurementIterations(1)
                .build();

        new Runner(opt).run();
    }

}
