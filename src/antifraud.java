import java.io.*;
import java.util.*;

/**
 * Created by jaydatta on 11/6/16.
 */
public class antifraud {
    /*
    * Dynamically setting MAX_DEPTH and number of threads
    * */

    public static int MAX_DEPTH = 5;
    public static int numThreads = 20;
    public static ArrayList<String> skipped = new ArrayList<>();
    static Map<String, Set<String>> transactionMap = new HashMap<>();
    private static String firstDegree;
    private static String secondDegree;
    private static String fourthDegree;
    private static int degree1 = 2;
    private static int degree2 = 3;
    private static int degree3 = 5;

    public static void main(String[] args) throws IOException {

        String batchFile = args[0];
        String streamFile = args[1];

        // Getting the file-names to store for n-degree connections
        firstDegree = args[2];
        secondDegree = args[3];
        fourthDegree = args[4];

        createGraph(batchFile);
        processQueries(streamFile);

    }

    /*
    * This method creates a Knowledge-graph of all connections and store in a data-structure
    *  Map <(Node) , Hashset<(Adjacent Nodes)>>
    *  HashMap and Hashset ensures o(1) lookup time.
    * */
    public static void createGraph(String batchFile) {

        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        try {
            br = new BufferedReader(new FileReader(batchFile));
            br.readLine();
            while ((line = br.readLine()) != null) {
                // use comma as separator
                if (line.contains(",")) {
                    String[] transaction = line.split(cvsSplitBy);
                    String user1 = transaction[1].trim();
                    String user2 = transaction[2].trim();

                    if (!transactionMap.containsKey(user1)) {
                        transactionMap.put(user1, new HashSet<>());
                    }

                    if (!transactionMap.containsKey(user2)) {
                        transactionMap.put(user2, new HashSet<>());
                    }

                    if (!transactionMap.get(user1).contains(user2)) {
                        transactionMap.get(user1).add(user2);
                    }

                    if (!transactionMap.get(user2).contains(user1)) {
                        transactionMap.get(user2).add(user1);
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /*
    *  This method reads all records in stream_input as an ArrayList and at run-time
    *  distributes the portions of ArrayList to be processed to 10-different threads.
    *
    * */
    public static void processQueries(String streamFile) throws IOException {

        BufferedReader br = null;
        String line = "";
        List<String> testTransactions = new ArrayList<>();
        try {
            br = null;
            br = new BufferedReader(new FileReader(streamFile));
            br.readLine();
            while ((line = br.readLine()) != null) {
                // use comma as separator
                if (line.contains(",")) {
                    testTransactions.add(line);                
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // Counts the size of arrayList and partition the arraylist in 10-partitions given to seperate threads
        int count = testTransactions.size();
        if (count < 20) {
            numThreads = count;
        }
        int eachThreadCount = count / numThreads;
        List<Thread> threadList = new ArrayList<>();
        int start = 0;
        List<ThreadLists> lists = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            ThreadLists l = new ThreadLists();
            lists.add(l);
            int end = Math.min(start + eachThreadCount, count);
            Thread t = new Thread(new antifraud.OtherThread(testTransactions, start, end, l), "thread " + i);
            threadList.add(t);
            start += eachThreadCount;
        }

        threadList.forEach(Thread::start);
        threadList.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        // Write processed results to 3 different Files
        writeToFile(firstDegree, secondDegree, fourthDegree, lists);
    }

    /*
    * Accepts the payer and payee and scans the payer's graph upto 2 levels.
    * Provided a lookup of 2 levels.
    *
    * */
    public static int isPresentBFS(String payerNode, String payeeNode, int currentLevel) {
        Queue<String> queue = new LinkedList<>();
        queue.add(payerNode);

        while (!queue.isEmpty() && currentLevel < (MAX_DEPTH - 2)) {
            int length = queue.size();
            for (int loop = 0; loop < length; loop++) {
                String currentNode = queue.remove();

                // Provides lookup of 1 further level
                if (transactionMap.get(currentNode).contains(payeeNode)) {
                    return currentLevel + 1;
                }
                // Provides lookup of 2 further levels
                for (String adj : transactionMap.get(currentNode)) {
                    if (transactionMap.get(adj).contains(payeeNode)) {
                        return currentLevel + 2;
                    }
                }
                // Provides lookup of the next level and adds it to queue for checking the next level.
                if (currentLevel != (MAX_DEPTH - 2)) {
                    for (String adjacentNode : transactionMap.get(currentNode)) {
                        if (adjacentNode.equals(payeeNode)) {
                            return (currentLevel);
                        }
                        queue.add(adjacentNode);
                    }
                }
            }
            currentLevel++;
        }

        // Program Control reaches only when level greater than MAX_LEVEL is obtained, so this is
        // always returns unverified (> MAX_DEGREE) results.
        return (currentLevel + 2);
    }

    /*
    * This method writes the final results to a file.
    *
    * */
    private static void writeToFile(String path1, String path2, String path3, List<ThreadLists> lists)
            throws IOException {

        try (FileWriter writer = new FileWriter(path1)) {
            for (ThreadLists list : lists) {
                for (String s : list.firstDegree) writer.write(s + "\n");
            }
        }

        try (FileWriter writer = new FileWriter(path2)) {
            for (ThreadLists list : lists) {
                for (String s : list.secondDegree) writer.write(s + "\n");
            }
        }

        try (FileWriter writer = new FileWriter(path3)) {
            for (ThreadLists list : lists) {
                for (String s : list.fourthDegree) writer.write(s + "\n");
            }
        }
    }

    /*
    * This method accepts the depth and based on that returns if the transaction is trusted or unverified.
    * */
    public static String[] printingArray(int depth) {
        String[] returnArray = new String[3];
        returnArray[0] = depth < degree1 ? "trusted" : "unverified";
        returnArray[1] = depth < degree2 ? "trusted" : "unverified";
        returnArray[2] = depth < degree3 ? "trusted" : "unverified";
        return returnArray;
    }

    /*
    * This class creates different list to write contents of threads into files.
    * */
    public static class ThreadLists {

        List<String> firstDegree;
        List<String> secondDegree;
        List<String> fourthDegree;

        ThreadLists() {
            firstDegree = new ArrayList<>();
            secondDegree = new ArrayList<>();
            fourthDegree = new ArrayList<>();
        }
    }

    private static class OtherThread implements Runnable {

        List<String> main;
        int start;
        int end;
        ThreadLists lists;

        public OtherThread(List<String> main, int start, int end, ThreadLists lists) {
            this.main = main;
            this.start = start;
            this.end = end;
            this.lists = lists;
        }

        @Override
        public void run() {
            String cvsSplitBy = ",";

            for (int i = start; i < end; i++) {
                String line = main.get(i);
                String[] transaction = line.split(cvsSplitBy);
                String user1 = transaction[1].trim();
                String user2 = transaction[2].trim();
                int depth;

                //Check exit conditions
                if (user1.equals(user2)) {
                    depth = 0;
                } else if (!transactionMap.containsKey(user1) || !transactionMap.containsKey(user2)) {
                    depth = Integer.MAX_VALUE;
                } else {
                    depth = isPresentBFS(user1, user2, 0);
                }
                String[] featureArray = printingArray(depth);
                lists.firstDegree.add(featureArray[0]);
                lists.secondDegree.add(featureArray[1]);
                lists.fourthDegree.add(featureArray[2]);
            }
        }
    }

}
