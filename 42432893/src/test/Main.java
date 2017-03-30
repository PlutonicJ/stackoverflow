package test;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {
        String source = "source";
        String target = "target";

        while (true) {
            List<Path> filePaths = filePathsList(source); // Step 1: get all files from a directory
            List<Path> filteredFilePaths = filter(filePaths); // Step 2: filter by ".txt"
            SortedMap<Path, List<String>> contentOfFiles = getContentOfFiles(filteredFilePaths); // Step 3: get content of files
            move(filteredFilePaths, target); // Step 4: move files to destination
            printToConsole(contentOfFiles);
            sendMessages(contentOfFiles);
            
            Thread.sleep(5000L);
        }
    }

    public static List<Path> filePathsList(String directory) throws IOException {
        List<Path> filePaths = new ArrayList<>();
        DirectoryStream<Path> directoryStream = Files.newDirectoryStream(FileSystems.getDefault().getPath(directory));
        for (Path path : directoryStream) {
            filePaths.add(path);
        }
        return filePaths;
    }

    private static List<Path> filter(List<Path> filePaths) {
        List<Path> filteredFilePaths = new ArrayList<>();
        for (Path filePath : filePaths) {
            if (filePath.getFileName().toString().endsWith(".txt")) {
                filteredFilePaths.add(filePath);
            }
        }
        return filteredFilePaths;
    }

    private static SortedMap<Path, List<String>> getContentOfFiles(List<Path> filePaths) throws IOException {
        SortedMap<Path, List<String>> contentOfFiles = new TreeMap<>((path1, path2) -> Long.valueOf(path1.toFile().lastModified()).compareTo(Long.valueOf(path2.toFile().lastModified())));
        for (Path filePath : filePaths) {
            contentOfFiles.put(filePath, new ArrayList<>());
            Files.readAllLines(filePath).forEach(contentOfFiles.get(filePath)::add);
        }
        return contentOfFiles;
    }

    private static void move(List<Path> filePaths, String target) throws IOException {
        Path targetDir = FileSystems.getDefault().getPath(target);
        if (!Files.isDirectory(targetDir)) {
            targetDir = Files.createDirectories(Paths.get(target));
        }
        for (Path filePath : filePaths) {
            System.out.println("Moving " + filePath.getFileName() + " to " + targetDir.toAbsolutePath());
            Files.move(filePath, Paths.get(target, filePath.getFileName().toString()), StandardCopyOption.ATOMIC_MOVE);
        }   
    }

    private static void printToConsole(SortedMap<Path, List<String>> contentOfFiles) {
        System.out.println("Content of files:");
        contentOfFiles.forEach((k,v) -> v.forEach(System.out::println));
    }
    
    private static void sendMessages(SortedMap<Path, List<String>> contentOfFiles) {
        ActiveMQConnectionFactory factory = null;
        Connection connection = null;
        Session session = null;
        
        try {
            factory = new ActiveMQConnectionFactory(ActiveMQConnectionFactory.DEFAULT_BROKER_URL);
            connection = factory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue("TestQueue");
            final MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            final Session sessionForLoop = session;
            contentOfFiles.forEach((k,v) -> v.forEach((text) -> Main.sendMessage(sessionForLoop, producer, text)));
            session.commit();
        } catch (JMSException e) {
            try {
                if (session != null) {
                    session.rollback();
                }
            } catch (JMSException ex) {
                // ignore
            }
        } finally {
            if (session != null) {
                try {
                    session.close();
                } catch (JMSException ex) {
                    // ignore
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException ex) {
                    // ignore
                }
            }
        }
    }
    
    public static void sendMessage(Session session, MessageProducer producer, String text) {
        try {
            TextMessage message = session.createTextMessage(text);
            producer.send(message);
        } catch (JMSException ex) {
            throw new RuntimeException(ex);
        }
    }
}