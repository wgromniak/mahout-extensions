package org.mimuw.attrsel;

import java.io.IOException;

/**
 * TODO: this does not work and does not have any tests
 */
// file located in "user.dir"/Data/csv
public class UserDataGenerator {
    public static void main(String[] args) throws IOException {
        if (args.length != 3)
        {
            System.out.println("Wrong args format, please enter: " +
                    "attibute number, row number, number of significant attributes");
        }
        else
        {
            DataGenerator dataGen = new DataGenerator(
                    Integer.parseInt(args[0]), Integer.parseInt(args[1]));
            dataGen.writeToCSV(dataGen.generateData(Integer.parseInt(args[2])));
        }
    }
}
