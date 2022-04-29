import java.io.File;
import java.io.FileWriter;
import java.util.*;

public class ETL_OLAP {

    /**
     * This is the main method of the ETL_OLAP program.
     */
    public static void main(String[] args) throws Exception {
        Map<String, Integer> countrySalesMap = new HashMap<>(); // HashMap for countrySalesMap

        File carSalesDataSet = new File("Car_Sales_Data_Set.csv");  // Reading CSV File
        Scanner scanner = new Scanner(carSalesDataSet); // Scanner for reading CSV data
        // Storing the csv data into ArrayList
        ArrayList<String[]> carSalesData = new ArrayList<String[]>();

        String[] header = scanner.nextLine().split(","); // Removes "," from thee header
        //Looping through the CSV file
        while (scanner.hasNext()) {
            carSalesData.add(scanner.nextLine().split(",")); // splits all the "," from csv file
        }
        scanner.close();    // Closing scanner

        //TODO: FIRST SORTING

        /**
         * This is the first sorting deals with the field of “Country”. Once it is done, records associated with
         * Canada are in front of records associated with United States. Now we have two sequential
         * groups of records: Canada_Set and US_Set. Note that both groups are still in the same
         * CSV file
         * */

        /*
         * A TreeSet of uniqueCountries is created and are looped through the CSV file to add data into the TreeSet.
         * */
        Set<String> uniqueCountries = new TreeSet<>();
        for (String[] car : carSalesData) {
            uniqueCountries.add(car[0]);    // Adds data to uniqueCountries TreeSet
        }
        /*
         * Nested loop through uniqueCountries and carSalesData then added to firstSortedList.
         * */
        ArrayList<String[]> firstSortedList = new ArrayList<>();
        for (String country : uniqueCountries) {
            int count = 0;
            for (String[] car : carSalesData) {
                if (country.equals(car[0])) {
                    firstSortedList.add(car);
                    count += Integer.parseInt(car[4]);
                }
            }
            countrySalesMap.put(country, count);
        }

        /*
         * Writing the firstSortedList result into the “Car_Sales_Data_Set_First_Sorting.csv” file.
         * */
        try {
            FileWriter fw = new FileWriter("Car_Sales_Data_Set_First_Sorting.csv");
            fw.write(header[0] + ',' + header[1] + ',' + header[2] + ',' + header[3] + ',' +header[4] + "\n");
            for (String[] c : firstSortedList) {
                fw.write( c[0] + ',' + c[1] + ',' + c[2] + ',' + c[3] + ',' + c[4] +"\n");
            }
            fw.flush(); // flushing the filewriter
            fw.close(); // closing the filewriter
        } catch (Exception e) {
            System.out.println("Error writing file : " + e.getStackTrace());    // error printing if any.
        }

        System.out.println("Saved the first sorting result in a file named “Car_Sales_Data_Set_First_Sorting.csv”");


        //TODO: SECOND SORTING

        /**
         * The second sorting is based on the result of the first sorting. The second sorting deals with
         * the field of “Time_Year”. Once it is done, records associated with Canada are still in front
         * of  records  associated  with  United  States;  however,  within  the  Canada_Set,  records
         * associated with 2017 are in front of records associated with 2018 (this also applies the
         * US_Set).  Now  we  have  four  sequential  groups  of  records  in  the  same  CSV  file:
         * Canada_2017_Set, Canada_2018_Set, US_2017_Set, US_2018_Set
         * */

        /*
         * A TreeSet of uniqueYear is created and are looped through the firstSortedList to add data into the TreeSet.
         * First the sorted countries is added to uniqueCountries and then sorted year is added to uniqueYear.
         * */
        Set<String> uniqueYear = new TreeSet<>();
        for (String[] car : firstSortedList) {
            uniqueCountries.add(car[0]);
            uniqueYear.add(car[1]);
        }

        /*
         * Nested looped through uniqueCountries, uniqueYear and firstSortedList then added to the secondSortedList.
         * */
        ArrayList<String[]> secondSortedList = new ArrayList<>();
        for (String country : uniqueCountries) {
            for (String year : uniqueYear) {
                int count = 0;
                for (String[] car : firstSortedList) {
                    if (country.equals(car[0])) {
                        if (year.equals(car[1])) {
                            secondSortedList.add(car);
                            count += Integer.parseInt(car[4]);
                        }
                    }
                }
            }
        }

        /*
         * Writing the firstSortedList result into the “Car_Sales_Data_Set_Second_Sorting.csv” file.
         * */
        try {
            FileWriter fw = new FileWriter("Car_Sales_Data_Set_Second_Sorting.csv");
            fw.write(header[0] + ',' + header[1] + ',' + header[2] + ',' + header[3] + ',' +header[4] + "\n");
            for (String[] c : secondSortedList) {
                fw.write( c[0] + ',' + c[1] + ',' + c[2] + ',' + c[3] + ',' + c[4] +"\n");
            }
            fw.flush(); // flushing thr filewriter
            fw.close(); // closing the filewriter
        } catch (Exception e) {
            System.out.println("Error writing file : " + e.getStackTrace());    // error printing if any.
        }

        System.out.println("Saved the sorting result in a file named “Car_Sales_Data_Set_Second_Sorting.csv”");

        //TODO: THIRD SORTING

        /**
         * The third sorting is based on the result of the second sorting. The third sorting deals with
         * the  field  of  “Time_Quarter”  and  it  does  not  change  the  order  of  the  data  groups
         * mentioned previously. However, within each data group (e.g. Canada_2017_Set), records
         * are sorted according to the quarter values (in ascending order, i.e. 1, 2, 3, 4). Now we have
         * sixteen  sequential  groups  of  records:  Canada_2017_1_Set,  Canada_2017_2_Set,  ...,
         * US_2018_3_ Set, US_2018_4_ Set.
         * */

        /*
         * A TreeSet of uniqueQuarter is created and are looped through the secondSortedList to add data into the TreeSet.
         * First the sorted countries is added to uniqueCountries then sorted year is added to uniqueYear, and then sorted year is added to uniqueYear.
         * */
        Set<String> uniqueQuarter = new TreeSet<>();
        for (String[] car : secondSortedList) {
            uniqueCountries.add(car[0]);
            uniqueYear.add(car[1]);
            uniqueQuarter.add(car[2]);
        }

        /*
         * Nested looped through uniqueCountries, uniqueYear,  uniqueQuarter and secondSortedList then added to the thirdSortedList.
         * */

        ArrayList<String[]> thirdSortedList = new ArrayList<>();
        for (String country : uniqueCountries) {
            for (String year : uniqueYear) {
                for (String quarter : uniqueQuarter) {
                    int count = 0;
                    for (String[] car : secondSortedList) {
                        if (country.equals(car[0])) {
                            if (year.equals(car[1])) {
                                if (quarter.equals(car[2])) {
                                    thirdSortedList.add(car);
                                    count += Integer.parseInt(car[4]);
                                }
                            }
                        }
                    }
                }

            }
        }

        try {
            FileWriter fw = new FileWriter("Car_Sales_Data_Set_Third_Sorting.csv");
            fw.write(header[0] + ',' + header[1] + ',' + header[2] + ',' + header[3] + ',' +header[4] + "\n");
            for (String[] c : thirdSortedList) {
                fw.write( c[0] + ',' + c[1] + ',' + c[2] + ',' + c[3] + ',' + c[4] +"\n");
            }
            fw.flush(); //flushing filwriter
            fw.close(); //closing filewriter
        } catch (Exception e) {
            System.out.println("Error writing file : " + e.getStackTrace());    //error printing if any.
        }

        Set<String> uniqueManufacturer = new TreeSet<>();
        for (String[] car : carSalesData) {
            uniqueManufacturer.add(car[3]);    // Adds data to uniqueCountries TreeSet
        }

        System.out.println("Saved the sorting result in a file named “Car_Sales_Data_Set_Third_Sorting.csv”");

        //TODO: ETL_OLAP displaying a list of 12 tuples
        //NOTE: Note that the sequence number associated with each tuple should also be displayed. Each of these tuples corresponds to a cuboid (i.e. a possible OLAP query) in the data cube.

        System.out.println("\n Please enter a valid number (i.e. a number in the range of 1-12), the program will process the corresponding OLAP query and display the result \n" +
                "1. () \n" +
                "2. (Country) \n" +
                "3. (Time_Year) \n" +
                "4. (Time_Quarter - Time_Year) \n" +
                "5. (Car_ Manufacturer) \n" +
                "6. (Country, Time_Year) \n" +
                "7. (Country, Time_Quarter - Time_Year) \n" +
                "8. (Country, Car_ Manufacturer) \n" +
                "9. (Time_Year, Car_ Manufacturer) \n" +
                "10. (Time_Quarter - Time_Year, Car_ Manufacturer) \n" +
                "11. (Country, Time_Year, Car_ Manufacturer) \n" +
                "12. (Country, Time_Quarter - Time_Year, Car_ Manufacturer)");

        // Getting user input for OLAP query
        Scanner scanner1 = new Scanner(System.in);      // Scanner for user input.
        System.out.println("Enter a valid number to perform OLAP query: ");
        int userInput = scanner1.nextInt(); // getting user input.

        /**
         *
         * */
        switch (userInput) {
//            TODO: 1. ()
            case 1:
                if (userInput == 1) {
                    System.out.println(header[4]);
                    int count = 0;
                    for (String[] car : firstSortedList) {
                        count += Integer.parseInt(car[4]);
                    }
                    System.out.println(count);
                }
                break;
//            TODO: 2. (Country)
            case 2:
                if (userInput == 2) {
                    System.out.println(header[0] + " " + header[4]);
                    for (String country : uniqueCountries) {
                        System.out.println(country + " " + countrySalesMap.get(country));
                    }
                }
                break;
//            TODO: 3. (Time_Year)
            case 3:
                if (userInput == 3) {
                    System.out.println(header[1] + " " + header[4]);

                    for (String year : uniqueYear) {
                        int count = 0;
                        for (String[] car : firstSortedList) {
                            if (year.equals(car[1])) {
                                count += Integer.parseInt(car[4]);
                            }
                        }
                        System.out.println(year + " " + count);
                    }
                }
                break;
//            TODO: 4. (Time_Quarter - Time_Year)
            case 4:
                if (userInput == 4) {
                    System.out.println(header[2] + " " + header[1] + " " + header[4]);

                    for (String quarter : uniqueQuarter) {
                        for (String year : uniqueYear) {
                            int count = 0;
                            for (String[] car : secondSortedList) {
                                if (quarter.equals(car[2])) {
                                    if (year.equals(car[1])) {
                                        count += Integer.parseInt(car[4]);
                                    }
                                }
                            }
                            System.out.println(quarter + "-" + year + " " + count);
                        }
                    }
                }
                break;
//            TODO: 5. (Car_ Manufacturer)
            case 5:
                if (userInput == 5) {

                    System.out.println(header[3] + " " + header[4]);

                    for (String manufacture : uniqueManufacturer) {
                        int count = 0;
                        for (String[] car : firstSortedList) {
                            if (manufacture.equals(car[3])) {
                                count += Integer.parseInt(car[4]);
                            }
                        }
                        System.out.println(manufacture + " " + count);
                    }
                }
                break;
//            TODO: 6. (Country, Time_Year)
            case 6:
                if (userInput == 6) {
                    // Country +  Time_Year  + Sales
                    System.out.println(header[0] + " " + header[1] + " " + header[4]);

                    for (String country : uniqueCountries) {
                        for (String year : uniqueYear) {
                            int count = 0;
                            for (String[] car : secondSortedList) {
                                if (country.equals(car[0])) {
                                    if (year.equals(car[1])) {
                                        count += Integer.parseInt(car[4]);
                                    }
                                }
                            }
                            System.out.println(country + " " + year + " " + count);
                        }
                    }

                }
                break;
//            TODO: 7. (Country, Time_Quarter - Time_Year)
            case 7:
                if (userInput == 7) {
                    // Country +  Time_Quarter - Time_Year  + Sales
                    System.out.println(header[0] + " " + header[2] + "-" + header[1] + " " + header[4]);

                    for (String country : uniqueCountries) {
                        for (String quarter : uniqueQuarter) {
                            for (String year : uniqueYear) {
                                int count = 0;
                                for (String[] car : secondSortedList) {
                                    if (country.equals(car[0])) {
                                        if (quarter.equals(car[2])) {
                                            if (year.equals(car[1])) {
                                                count += Integer.parseInt(car[4]);
                                            }
                                        }
                                    }
                                }
                                System.out.println(country + " " + quarter + "-" + year + " " + count);
                            }
                        }

                    }

                }
                break;

//            TODO: 8. (Country, Car_ Manufacturer)
            case 8:
                if (userInput == 8) {
                    // Country +  Car_ Manufacturer   + Sales
                    System.out.println(header[0] + " " + header[3] + " " + header[4]);

                    for (String country : uniqueCountries) {
                        for (String manufacture : uniqueManufacturer) {
                            int count = 0;
                            for (String[] car : firstSortedList) {
                                if (manufacture.equals(car[3])) {
                                    count += Integer.parseInt(car[4]);
                                }
                            }
                            System.out.println(country + " " + manufacture + " " + count);
                        }

                    }
                }
                break;
//            TODO: 9. (Time_Year, Car_ Manufacturer)
            case 9:
                if (userInput == 9) {
                    //  Time_Year  + Car_ Manufacturer + Sales
                    System.out.println(header[1] + " " + header[3] + " " + header[4]);

                    for (String year : uniqueYear) {
                        for (String manufacture : uniqueManufacturer) {
                            int count = 0;
                            for (String[] car : secondSortedList) {
                                if (year.equals(car[1])) {
                                    if (manufacture.equals(car[3])) {
                                        count += Integer.parseInt(car[4]);
                                    }
                                }
                            }
                            System.out.println(year + " " + manufacture + " " + count);
                        }
                    }
                }
                break;
//            TODO: 10. (Time_Quarter - Time_Year, Car_ Manufacturer)
            case 10:
                if (userInput == 10) {
                    //  Time_Quarter - Time_Year  + Car_ Manufacturer + Sales
                    System.out.println(header[2] + "-" + header[1] + " " + header[3] + " " + header[4]);

                    for (String year : uniqueYear) {
                        for (String quarter : uniqueQuarter) {
                            for (String manufacturer : uniqueManufacturer) {
                                int count = 0;
                                for (String[] car : thirdSortedList) {
                                    if (year.equals(car[1])) {
                                        if (quarter.equals(car[2])) {
                                            if (manufacturer.equals(car[3])) {
                                                count += Integer.parseInt(car[4]);
                                            }
                                        }
                                    }
                                }
                                System.out.println(quarter + "-" + year + " " + manufacturer + " " + count);
                            }
                        }
                    }
                }
                break;
//            TODO: 11. (Country, Time_Year, Car_ Manufacturer)
            case 11:
                if (userInput == 11) {
                    System.out.println(header[0] + " " + header[1] + " " + header[3] + " " + header[4]);

                    for (String country : uniqueCountries) {
                        for (String year : uniqueYear) {
                            for (String manufacturer : uniqueManufacturer) {
                                int count = 0;
                                for (String[] car : thirdSortedList) {
                                    if (country.equals(car[0])) {
                                        if (year.equals(car[1])) {
                                            if (manufacturer.equals(car[3])) {
                                                count += Integer.parseInt(car[4]);
                                            }
                                        }
                                    }
                                }
                                System.out.println(country + "-" + year + " " + manufacturer + " " + count);
                            }
                        }
                    }
                }
                break;

//            TODO: 12. (Country, Time_Quarter - Time_Year, Car_ Manufacturer)
            case 12:
                if (userInput == 12) {
                    System.out.println(header[0] + " " + header[2] + " " + header[1] + " " + header[3] + " " + header[4]);

                    for (String country : uniqueCountries) {
                        for (String quarter : uniqueQuarter) {
                            for (String year : uniqueYear) {
                                for (String manufacturer : uniqueManufacturer) {
                                    int count = 0;
                                    for (String[] car : thirdSortedList) {
                                        if (country.equals(car[0])) {
                                            if (quarter.equals(car[2])) {
                                                if (year.equals(car[1])) {
                                                    if (manufacturer.equals(car[3])) {
                                                        count += Integer.parseInt(car[4]);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    System.out.println(country + " " + quarter + "-" + year + " " + manufacturer + " " + count);
                                }
                            }
                        }
                    }
                }
                break;
            default:
                System.out.println("Invalid number");   // If user enters invalid number out of 1-12 scope.
        }   // Switch
    }   // Main
}   // Class