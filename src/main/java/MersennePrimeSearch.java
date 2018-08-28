import mpi.Comm;
import mpi.MPI;
import mpi.MPIException;
import mpi.Status;

import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Mersenne Prime Search
 * <p>
 Mersenne numbers are numbers that can be written in the form 2^n - 1.  They are named after Marin Mersenne, a French Mathematician who studied them in the 17th century. Mersenne numbers were proven to be a way to find large prime numbers.  This program implements a distributed application that searches for Mersenne prime numbers.
 *
 * @date 2018-05-29
 * @version 1.0
 * @author Timothy Murphy
 * @author Thyago Mota (advisor)
 */

public class MersennePrimeSearch {

    private static Logger logger;
    private static       Comm   comm;
    private static       int    np;
    private static       int    cores;
    public  static final String LOG_FILE_NAME             = "mersenne_prime_search";
    public  static final double QUEUE_CAPACITY_FACTOR     = 1.5;
    public  static final int    DEFAULT_INITIAL_EXPONENT  = 100000000;
    public  static final int    MASTER_RANK               = 0;
    public  static final int    MAX_BUFFER_SIZE           = 1024;
    public  static final int    REQUEST_TASK_MESSAGE_TAG  = 0;
    public  static final int    TASK_MESSAGE_TAG          = 1;
    public  static final int    RESPONSE_MESSAGE_TAG      = 2;

    public static void runMaster(final int initialExponent) throws MPIException {
        // queue settings
        int capacity = (int) Math.round(np * cores * QUEUE_CAPACITY_FACTOR);
        logger.info("Starting task queue with capacity = " + capacity);
        final BlockingQueue<Task> tasks = new ArrayBlockingQueue<Task>(capacity);

        // start producer thread in background
        logger.info("Producer thread started in the background");
        new Thread(new Runnable() {
            public void run() {
                int exponent = initialExponent;
                while (true) {
                    Task task = new Task(exponent);
                    try {
                        tasks.put(task);
                    } catch (InterruptedException ex) {
                        System.out.println(ex);
                    }
                    if (exponent == Integer.MAX_VALUE)
                        break;
                    exponent++;
                }
            }
        }).start();

        Status status;
        byte buffer[];
        logger.info("Master thread main loop started");
        while (true) {
            try {

                // check for task request messsages
                for (int i = 1; i < np; i++) {
                    status = comm.iProbe(i, REQUEST_TASK_MESSAGE_TAG);
                    if (status != null) {
                        buffer = new byte[1];
                        comm.recv(buffer, 1, MPI.BYTE, i, REQUEST_TASK_MESSAGE_TAG);
                        Task task = tasks.take();
                        buffer = Task.serialize(task);
                        comm.send(buffer, buffer.length, MPI.BYTE, i, TASK_MESSAGE_TAG);
                        // logger.info("Slave process #" + i + " was sent " + task);
                    }
                }

                // check for response messages
                for (int i = 1; i < np; i++) {
                    status = comm.iProbe(i, RESPONSE_MESSAGE_TAG);
                    if (status != null) {
                        buffer = new byte[MAX_BUFFER_SIZE];
                        comm.recv(buffer, MAX_BUFFER_SIZE, MPI.BYTE, i, RESPONSE_MESSAGE_TAG);
                        Task task = Task.deserialize(buffer);
                        if (task.isPrime())
                            logger.info(task.toString());
                    }
                }
            }
            catch (InterruptedException | IOException | ClassNotFoundException ex) {
                System.out.println(ex);
            }
        }
    }

    public static void runMaster() throws MPIException {
        runMaster(DEFAULT_INITIAL_EXPONENT);
    }

    public static void runSlave() throws MPIException {
        // start a separate thread on each core
        Thread t[] = new Thread[cores];
        for (int i = 0; i < cores; i++) {
            t[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    byte buffer[];
                    while (true) {
                        try {

                            // request a task
                            buffer = new byte[1];
                            comm.send(buffer, 1, MPI.BYTE, MASTER_RANK, REQUEST_TASK_MESSAGE_TAG);

                            // receive a task
                            buffer = new byte[MAX_BUFFER_SIZE];
                            comm.recv(buffer, MAX_BUFFER_SIZE, MPI.BYTE, MASTER_RANK, TASK_MESSAGE_TAG);
                            Task task = Task.deserialize(buffer);

                            logger.fine("Thread " + Thread.currentThread().getName() + " is working on " + task);

                            // run "Lucas–Lehmer" primality test
                            // see: https://en.wikipedia.org/wiki/Lucas%E2%80%93Lehmer_primality_test
                            int exponent = task.getExponent();
                            BigInteger s = BigInteger.valueOf(4);
                            BigInteger two = new BigInteger("2");
                            BigInteger mersenneNumber = two.pow(exponent);
                            mersenneNumber = mersenneNumber.subtract(BigInteger.ONE);
                            for (int i = 0; i < exponent - 2; i++)
                                s = ((s.multiply(s)).subtract(two)).mod(mersenneNumber);
                            if (s.equals(BigInteger.valueOf(0))) {
                                task.setPrime(true);
                                logger.info("Thread \"" + Thread.currentThread().getName() + "\" is reporting " + task);
                            }

                            // send response to master process
                            buffer = Task.serialize(task);
                            comm.send(buffer, buffer.length, MPI.BYTE, MASTER_RANK, RESPONSE_MESSAGE_TAG);
                        }
                        catch (IOException | ClassNotFoundException | MPIException ex) {
                            System.out.println(ex);
                        }
                    }
                }
            });
            t[i].setName("rank: " + comm.getRank() + ", core: " + i);
            t[i].start();
            logger.info("Thread " + t[i].getName() + " started");
        }

        // wait for all theads to finish
        System.out.println("Waiting for all threads to finish...");
        for (int i = 0; i < cores; i++) {
            try {
                t[i].join();
            } catch (InterruptedException ex) {

            }
        }
        System.out.println("Not supposed to be here...");
    }

    public static void main(String[] args) throws MPIException {
        logger = Logger.getLogger(LOG_FILE_NAME);
        logger.setLevel(Level.INFO);
        MPI.InitThread(args, MPI.THREAD_MULTIPLE);
        comm  = MPI.COMM_WORLD;
        np    = comm.getSize();
        cores = Runtime.getRuntime().availableProcessors();
        cores  = 2;
        if (comm.getRank() == MASTER_RANK)
            runMaster(2);
        else
            runSlave();
        MPI.Finalize();
    }
}
