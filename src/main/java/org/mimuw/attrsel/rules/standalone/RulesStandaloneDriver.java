package org.mimuw.attrsel.rules.standalone;

import org.apache.mahout.math.Matrix;
import org.mimuw.attrsel.common.CSVMatrixReader;
import org.mimuw.attrsel.common.TreeAccuracyValidator;
import org.mimuw.attrsel.common.api.Subtable;
import org.mimuw.attrsel.common.api.SubtableGenerator;
import org.mimuw.attrsel.reducts.AbstractAttrSelReductsDriver;
import org.mimuw.attrsel.rules.RulesGenerator;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


final class RulesStandaloneDriver extends AbstractAttrSelReductsDriver {

    private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    @Override
    public int run(String... args) throws Exception {
        setUpAttrSelOptions();
        setUpReductsOptions();
        if (parseArguments(args, false, true) == null) {
            return 1;
        }

        Matrix inputDataTable = new CSVMatrixReader().read(Paths.get(getInputFile().getPath()));

        SubtableGenerator<Subtable> subtableGenerator = getSubtableGenerator(inputDataTable);

        List<Subtable> subtables = subtableGenerator.getSubtables();


        List<Callable<List<Integer>>> map = new ArrayList<>(subtables.size());

        for (final Subtable subtable : subtables) {
            map.add(new Callable<List<Integer>>() {
                @Override
                public List<Integer> call() throws Exception {
                    RulesGenerator rules = new RulesGenerator();
                    return rules.calculateScoresOfAttributes(rules.generateRules(subtable), subtable.getNumberOfAttributes());

                }
            });
        }

        List<Future<List<Integer>>> mapResults = executor.invokeAll(map);

        int[] attrCounts = new int[inputDataTable.columnSize()];
        int numberAttribute;

        for (Future<List<Integer>> result : mapResults) {
            numberAttribute = 0;
            for (Integer rul : result.get()) {
                attrCounts[numberAttribute] = attrCounts[numberAttribute] + rul;
                numberAttribute++;
            }
        }

        TreeMap<Integer,Integer> mapScoreAttribute = new TreeMap<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o2-o1;
            }
        });

        for(int i  = 0; i < attrCounts.length; i++) {
            mapScoreAttribute.put(attrCounts[i], i);
        }

        List<Integer> selected = cutAttributes(mapScoreAttribute);

        System.out.printf("Selected attrs: %s%n", selected);
        System.out.printf("Num selected attrs: %s%n", selected.size());

        double acc = new TreeAccuracyValidator().validate(inputDataTable, selected);

        System.out.printf("Accuracy: %s%n", acc);

        executor.shutdown();

        return 0;
    }

    private List<Integer> cutAttributes(TreeMap<Integer, Integer> mapa) {
        int cutPoint = 6;
        List<Integer> selected =new ArrayList<>();
        for (Integer it : mapa.values()) {
            if(selected.size()<cutPoint){
                selected.add(it);
            }
        }
        return selected;
    }


    public static void main(String... args) throws Exception {

      //  long startTime = System.nanoTime();
        //new RulesStandaloneDriver().run(args);
        new RulesStandaloneDriver().run("-i", "input/train.csv", "-numSub", "1", "-subCard", "1");
        //long endTime = System.nanoTime();
        //System.out.println((double)((endTime-startTime)/1000000));

    }
}
