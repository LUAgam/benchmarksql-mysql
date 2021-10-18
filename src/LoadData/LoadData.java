/*
 * LoadData - Load Sample Data directly into database tables or into
 * CSV files using multiple parallel workers.
 *
 * Copyright (C) 2016, Denis Lussier
 * Copyright (C) 2016, Jan Wieck
 *
 */

import fs.FileNio;
import fs.FileUtils;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.*;
import java.util.*;
import java.io.*;
import java.lang.Integer;

public class LoadData {
    private static Properties ini = new Properties();
    private static String db;
    private static Properties dbProps;
    private static jTPCCRandom rnd;
    private static String fileLocation = null;
    private static String csvNullValue = null;

    private static int numWarehouses;
    private static int numWorkers;
    private static int nextJob = 0;
    private static Object nextJobLock = new Object();

    private static LoadDataWorker[] workers;
    private static Thread[] workerThreads;

    private static String[] argv;

    private static boolean writeCSV = false;
    //    private static BufferedWriter configCSV = null;
//    private static BufferedWriter itemCSV = null;
//    private static BufferedWriter warehouseCSV = null;
//    private static BufferedWriter districtCSV = null;
//    private static BufferedWriter stockCSV = null;
//    private static BufferedWriter customerCSV = null;
//    private static BufferedWriter historyCSV = null;
//    private static BufferedWriter orderCSV = null;
//    private static BufferedWriter orderLineCSV = null;
//    private static BufferedWriter newOrderCSV = null;
    private static FileChannel configCSV = null;
    private static FileChannel itemCSV = null;
    private static FileChannel warehouseCSV = null;
    private static FileChannel districtCSV = null;
    private static FileChannel stockCSV = null;
    private static FileChannel customerCSV = null;
    private static FileChannel historyCSV = null;
    private static FileChannel orderCSV = null;
    private static FileChannel orderLineCSV = null;
    private static FileChannel newOrderCSV = null;

    public static void main(String[] args) {
        int i;

        System.out.println("Starting BenchmarkSQL LoadData");
        System.out.println("");

        /*
         * Load the Benchmark properties file.
         */
        try {
            ini.load(new FileInputStream(System.getProperty("prop")));
        } catch (IOException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
        argv = args;

        /*
         * Initialize the global Random generator that picks the
         * C values for the load.
         */
        rnd = new jTPCCRandom();

        /*
         * Load the JDBC driver and prepare the db and dbProps.
         */
        try {
            Class.forName(iniGetString("driver"));
        } catch (Exception e) {
            System.err.println("ERROR: cannot load JDBC driver - " +
                    e.getMessage());
            System.exit(1);
        }
        db = iniGetString("conn");
        dbProps = new Properties();
        dbProps.setProperty("user", iniGetString("user"));
        dbProps.setProperty("password", iniGetString("password"));

        /*
         * Parse other vital information from the props file.
         */
        numWarehouses = iniGetInt("warehouses");
        numWorkers = iniGetInt("loadWorkers", 4);
        fileLocation = iniGetString("fileLocation");
        csvNullValue = iniGetString("csvNullValue", "NULL");

        /*
         * If CSV files are requested, open them all.
         */
        if (fileLocation != null) {
            writeCSV = true;

            try {
//                configCSV = new BufferedWriter(new FileWriter(fileLocation +
//                        "config.csv"));
//                itemCSV = new BufferedWriter(new FileWriter(fileLocation +
//                        "item.csv"));
//                warehouseCSV = new BufferedWriter(new FileWriter(fileLocation +
//                        "warehouse.csv"));
//                districtCSV = new BufferedWriter(new FileWriter(fileLocation +
//                        "district.csv"));
//                stockCSV = new BufferedWriter(new FileWriter(fileLocation +
//                        "stock.csv"));
//                customerCSV = new BufferedWriter(new FileWriter(fileLocation +
//                        "customer.csv"));
//                historyCSV = new BufferedWriter(new FileWriter(fileLocation +
//                        "cust-hist.csv"));
//                orderCSV = new BufferedWriter(new FileWriter(fileLocation +
//                        "order.csv"));
//                orderLineCSV = new BufferedWriter(new FileWriter(fileLocation +
//                        "order-line.csv"));
//                newOrderCSV = new BufferedWriter(new FileWriter(fileLocation +
//                        "new-order.csv"));
                configCSV = new FileNio(fileLocation + "config.csv", "rw");
                itemCSV = new FileNio(fileLocation + "item.csv", "rw");
                warehouseCSV = new FileNio(fileLocation + "warehouse.csv", "rw");
                districtCSV = new FileNio(fileLocation + "district.csv", "rw");
                stockCSV = new FileNio(fileLocation + "stock.csv", "rw");
                customerCSV = new FileNio(fileLocation + "customer.csv", "rw");
                historyCSV = new FileNio(fileLocation + "cust-hist.csv", "rw");
                orderCSV = new FileNio(fileLocation + "order.csv", "rw");
                orderLineCSV = new FileNio(fileLocation + "order-line.csv", "rw");
                newOrderCSV = new FileNio(fileLocation + "new-order.csv", "rw");
            } catch (IOException ie) {
                System.err.println(ie.getMessage());
                System.exit(3);
            }
        }

        System.out.println("");

        /*
         * Create the number of requested workers and start them.
         */
        workers = new LoadDataWorker[numWorkers];
        workerThreads = new Thread[numWorkers];
        for (i = 0; i < numWorkers; i++) {
            Connection dbConn;

            try {
                dbConn = DriverManager.getConnection(db, dbProps);
                dbConn.setAutoCommit(false);
                if (writeCSV)
                    workers[i] = new LoadDataWorker(i, csvNullValue,
                            rnd.newRandom());
                else
                    workers[i] = new LoadDataWorker(i, dbConn,
                            rnd.newRandom());
                workerThreads[i] = new Thread(workers[i]);
                workerThreads[i].start();
            } catch (SQLException se) {
                System.err.println("ERROR: " + se.getMessage());
                System.exit(3);
                return;
            }

        }

        for (i = 0; i < numWorkers; i++) {
            try {
                workerThreads[i].join();
            } catch (InterruptedException ie) {
                System.err.println("ERROR: worker " + i + " - " +
                        ie.getMessage());
                System.exit(4);
            }
        }

        /*
         * Close the CSV files if we are writing them.
         */
        if (writeCSV) {
            try {
                configCSV.close();
                itemCSV.close();
                warehouseCSV.close();
                districtCSV.close();
                stockCSV.close();
                customerCSV.close();
                historyCSV.close();
                orderCSV.close();
                orderLineCSV.close();
                newOrderCSV.close();
            } catch (IOException ie) {
                System.err.println(ie.getMessage());
                System.exit(3);
            }
        }
    } // End of main()

    public static void configAppend(StringBuffer buf)
            throws IOException {
        synchronized (configCSV) {
            configCSV.write(ByteBuffer.wrap(buf.toString().getBytes()));
        }
        buf.setLength(0);
    }

    public static void itemAppend(StringBuffer buf)
            throws IOException {
        synchronized (itemCSV) {
            itemCSV.write(ByteBuffer.wrap(buf.toString().getBytes()));
        }
        buf.setLength(0);
    }

    public static void warehouseAppend(StringBuffer buf)
            throws IOException {
        synchronized (warehouseCSV) {
            warehouseCSV.write(ByteBuffer.wrap(buf.toString().getBytes()));
        }
        buf.setLength(0);
    }

    public static void districtAppend(StringBuffer buf)
            throws IOException {
        synchronized (districtCSV) {
            districtCSV.write(ByteBuffer.wrap(buf.toString().getBytes()));
        }
        buf.setLength(0);
    }

    public static void stockAppend(StringBuffer buf)
            throws IOException {
        synchronized (stockCSV) {
            stockCSV.write(ByteBuffer.wrap(buf.toString().getBytes()));
        }
        buf.setLength(0);
    }

    public static void customerAppend(StringBuffer buf)
            throws IOException {
        synchronized (customerCSV) {
            customerCSV.write(ByteBuffer.wrap(buf.toString().getBytes()));
        }
        buf.setLength(0);
    }

    public static void historyAppend(StringBuffer buf)
            throws IOException {
        synchronized (historyCSV) {
            historyCSV.write(ByteBuffer.wrap(buf.toString().getBytes()));
        }
        buf.setLength(0);
    }

    public static void orderAppend(StringBuffer buf)
            throws IOException {
        synchronized (orderCSV) {
            orderCSV.write(ByteBuffer.wrap(buf.toString().getBytes()));
        }
        buf.setLength(0);
    }

    public static void orderLineAppend(StringBuffer buf)
            throws IOException {
        synchronized (orderLineCSV) {
            orderLineCSV.write(ByteBuffer.wrap(buf.toString().getBytes()));
        }
        buf.setLength(0);
    }

    public static void newOrderAppend(StringBuffer buf)
            throws IOException {
        synchronized (newOrderCSV) {
            newOrderCSV.write(ByteBuffer.wrap(buf.toString().getBytes()));
        }
        buf.setLength(0);
    }

    public static int getNextJob() {
        int job;

        synchronized (nextJobLock) {
            if (nextJob > numWarehouses)
                job = -1;
            else
                job = nextJob++;
        }

        return job;
    }

    public static int getNumWarehouses() {
        return numWarehouses;
    }

    private static String iniGetString(String name) {
        String strVal = null;

        for (int i = 0; i < argv.length - 1; i += 2) {
            if (name.toLowerCase().equals(argv[i].toLowerCase())) {
                strVal = argv[i + 1];
                break;
            }
        }

        if (strVal == null)
            strVal = ini.getProperty(name);

        if (strVal == null)
            System.out.println(name + " (not defined)");
        else if (name.equals("password"))
            System.out.println(name + "=***********");
        else
            System.out.println(name + "=" + strVal);
        return strVal;
    }

    private static String iniGetString(String name, String defVal) {
        String strVal = null;

        for (int i = 0; i < argv.length - 1; i += 2) {
            if (name.toLowerCase().equals(argv[i].toLowerCase())) {
                strVal = argv[i + 1];
                break;
            }
        }

        if (strVal == null)
            strVal = ini.getProperty(name);

        if (strVal == null) {
            System.out.println(name + " (not defined - using default '" +
                    defVal + "')");
            return defVal;
        } else if (name.equals("password"))
            System.out.println(name + "=***********");
        else
            System.out.println(name + "=" + strVal);
        return strVal;
    }

    private static int iniGetInt(String name) {
        String strVal = iniGetString(name);

        if (strVal == null)
            return 0;
        return Integer.parseInt(strVal);
    }

    private static int iniGetInt(String name, int defVal) {
        String strVal = iniGetString(name);

        if (strVal == null)
            return defVal;
        return Integer.parseInt(strVal);
    }
}
