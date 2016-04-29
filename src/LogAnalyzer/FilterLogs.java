/**
 * Created by chrislin on 4/10/16.
 */
package LogAnalyzer;

import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.Collections;
import java.util.Comparator;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.io.EOFException;


// Filters logs by performing external merge sort
public class FilterLogs {

    // estimateBlockSize divides files into small blocks inorder to fit in memory
    //
    // @param log file
    // @return block size

    public static long estimateBlockSize(File file) {
        long sizeOfFile = file.length();
        // We don't want to create to many block files
        // so the cap is set at 1000 blocks (< 1000 may be too many).
        final int maxNumberOfBlocks = 1000;
        long blockSize = sizeOfFile / maxNumberOfBlocks ;
        // In order of not wasting unused memory,
        // if blocks are less that half of JVM free memory, increase block size.
        long freeMemory = Runtime.getRuntime().freeMemory();
        if( blockSize < freeMemory/2){
            blockSize = freeMemory/2;
        } else {
            if(blockSize >= freeMemory) {
                System.err.println("Expect for memory to run out");
            }
        }
        return blockSize;
    }



    // sortIntoBlocks method will load file by blocks.
    // Each file will then be sorted in-memory and saved on disk to be later merged
    //
    // @param log file
    // @parm comparator to compare and sort string
    // @return A list of files saved in blocks

    public static ArrayList<File> sortIntoBlocks(File file, Comparator<String> comparator) throws IOException {
        ArrayList<File> onDiskBlocks = new ArrayList<File>();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        ArrayList<String> block =  new ArrayList<String>();
        String line = "";
        // Retrieve block size in bytes
        long blockSize = estimateBlockSize(file);
        try{
            try {
                while(line != null) {
                    long currBlockSize = 0;
                    // Fill block up to block size
                    while((currBlockSize < blockSize) &&((line = bufferedReader.readLine()) != null)) {
                        block.add(line);
                        // Java uses 2 bytes for each char + 30 (approximated) overhead
                        currBlockSize += line.length() * 2 + 30;
                    }
                    onDiskBlocks.add(sortAndSave(block, comparator));
                    block.clear();
                }
            } catch(EOFException e) {
                if(block.size()>0) {
                    onDiskBlocks.add(sortAndSave(block, comparator));
                    block.clear();
                }
            }
        } finally {
            bufferedReader.close();
        }
        return onDiskBlocks;
    }



    // sortAndSave takes given block, sorts it based on userid and writes file to disk
    //
    // @param block file
    // @param comparator
    // @return block file that has been sorted and stored onto disk

    public static File sortAndSave(ArrayList<String> block, Comparator<String> comparator) throws IOException {
        Collections.sort(block, comparator);
        File blockToDiskFile = File.createTempFile("sortedBlocks", ".log");
        blockToDiskFile.deleteOnExit();
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(blockToDiskFile));
        try {
            for(String b: block) {
                bufferedWriter.write(b);
                bufferedWriter.newLine();
            }
        } finally {
            bufferedWriter.close();
        }
        return blockToDiskFile;
    }


    // mergeSortedFiles obtains files saved on disk and merge/sort saved files using a PriorityQueue into output file
    //
    // @param onDiskBlocks (on disk file blocks)
    // @param output file

    public static void mergeSortedFiles(ArrayList<File> onDiskBlocks, final Comparator<String> comparator) throws IOException {
        // PriorityQueue constructed with default capacity of 11 and comparator
        PriorityQueue<FileBuffer> priorityQueue = new PriorityQueue<>(11, new Comparator<FileBuffer>() {
                    public int compare(FileBuffer a, FileBuffer b) {
                        return comparator.compare(a.peek(), b.peek());
                    }
                });
        // put disk blocks on priority queue
        for (File f : onDiskBlocks) {
            FileBuffer fileBuffer = new FileBuffer(f);
            priorityQueue.add(fileBuffer);
        }

        File outputFileName = new File("out123.log");
        outputFileName.deleteOnExit();
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFileName));
        try {
            // merges all blocks into a sorted output file by popping off priority queue
            while (priorityQueue.size() > 0) {
                FileBuffer fileBuffer = priorityQueue.poll();
                String file = fileBuffer.pop();
                bufferedWriter.write(file);
                bufferedWriter.newLine();
                if (fileBuffer.empty()) {
                    fileBuffer.bufferedReader.close();
                    fileBuffer.originalfile.delete();
                } else {
                    // add back to priority queue if not empty
                    priorityQueue.add(fileBuffer);
                }
            }
        } finally {
            bufferedWriter.close();
            for(FileBuffer fileBuffer : priorityQueue) {
                fileBuffer.close();
            }
        }
    }


    // findUsers reads through sorted file and stores userids that have visited at least n distinct paths.
    //
    // @param n (n distinct paths)
    // @param output file

    public static void findUsers(int n) throws IOException{
        String outputDir = System.getProperty("user.dir") + "output.log";
        BufferedReader bufferedReader = new BufferedReader(new FileReader("out123.log"));
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputDir));
        String line = "";
        String prevLine = "";
        int counter = 1;
        try {
            if((prevLine = bufferedReader.readLine()) != null) {
                String[] prevLineSplit = prevLine.split(",");
                String prev = prevLineSplit[1];
                while ((line = bufferedReader.readLine()) != null) {
                    String[] split = line.split(",");
                    String userid = split[1];
                    if (userid.equals(prev)) {
                        counter++;
                    } else {
                        if (counter >= n) {
                            bufferedWriter.write(userid);
                            bufferedWriter.newLine();
                            counter = 0;
                        }
                    }
                    prev = userid;
                }
            }
        } finally {
            bufferedReader.close();
            bufferedWriter.close();
        }
    }


    public static void main(String[] args) throws IOException {
        if(args.length < 2) {
            System.out.println("Please provide input filename and number of distinct path for user");
            return;
        }
        String inputfile = args[0];
        String n = args[1];
        Comparator<String> comparator = new Comparator<String>() {
            public int compare(String r1, String r2){
                String[] split1 = r1.split(",");
                String[] split2 = r2.split(",");
                String s1 = split1[1];
                String s2 = split2[1];
                return s1.compareTo(s2);
            }
        };
        ArrayList<File> onDiskBlocks = sortIntoBlocks(new File(inputfile), comparator) ;
        mergeSortedFiles(onDiskBlocks, comparator);
        findUsers(Integer.parseInt(n));
    }
}


// class that defines bufferedReader functionality
class FileBuffer {
    public static int BufferSize = 2048;
    public BufferedReader bufferedReader;
    public File originalfile;
    private String cache;
    private boolean empty;

    public FileBuffer(File file) throws IOException {
        originalfile = file;
        bufferedReader = new BufferedReader(new FileReader(file), BufferSize);
        reload();
    }

    public boolean empty() {
        return empty;
    }

    private void reload() throws IOException {
        try {
            if((this.cache = bufferedReader.readLine()) == null){
                empty = true;
                cache = null;
            }
            else{
                empty = false;
            }
        } catch(EOFException oef) {
            empty = true;
            cache = null;
        }
    }

    public void close() throws IOException {
        bufferedReader.close();
    }

    public String peek() {
        if(empty()) return null;
        return cache.toString();
    }

    public String pop() throws IOException {
        String answer = peek();
        reload();
        return answer;
    }
}
