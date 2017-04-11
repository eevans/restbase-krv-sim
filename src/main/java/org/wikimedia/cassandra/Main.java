package org.wikimedia.cassandra;

import static com.codahale.metrics.MetricRegistry.name;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.ini4j.Ini;
import org.ini4j.Profile.Section;
import org.slf4j.LoggerFactory;
import org.wikimedia.cassandra.Util.AltValue;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Cli;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.model.GlobalMetadata;
import com.google.common.base.Throwables;

@Cli(name = "krv-simulator", defaultCommand = Main.Help.class, commands = {
        Main.Write.class,
        Main.AltWrite.class,
        Main.Read.class,
        Main.AltRead.class,
        Main.Schema.class,
        Main.GetAltValue.class,
        Main.Help.class })
public class Main {

    abstract static class Cmd implements Runnable {
        @Option(name = { "--host" }, description = "Cassandra hostname (default: 127.0.0.1)")
        String host = "127.0.0.1";

        @Option(name = { "--port" }, description = "Cassandra port (default: 9042)")
        int port = 9042;

        @Option(name = {"--ssl"}, description = "Enable client encryption, (also requires you set JSSE properties appropriately, see: http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html#Customization)")
        boolean ssl = false;

        @Option(name = {"--cqlshrc"}, description = "Parse authentication credentials from a cqlshrc file")
        String cqlshrc;

        @Inject
        HelpOption<Cmd> help;

        String contact() {
            return String.format(this.host, this.port);
        }

        CassandraSession.Builder sessionBuilder() {
            CassandraSession.Builder builder = CassandraSession.builder().addContactPoints(this.contact())
                    .withSSL(this.ssl);

            if (this.cqlshrc != null) {
                try {
                    Ini rc = new Ini(new File(this.cqlshrc));
                    Section creds = checkNotNull(rc.get("authentication"), "cqlshrc file missing authentication section");
                    String username = checkNotNull(creds.get("username"), "unable to parse username from cqlshrc file");
                    String password = checkNotNull(creds.get("password"), "unable to parse password from cqlshrc file");
                    builder.withCredentials(username, password);
                }
                catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }

            return builder;
        }
    }

    @Command(name = "write", description = "Write new revisions")
    public static class Write extends Cmd {
        @Option(name = { "-np", "--num-partitions" }, description = "Number of partitions to write (default: 1000)")
        private int numPartitions = 1000;
        @Option(name = { "-po", "--partition-offset" }, description = "Partition offset to start from (default: 0)")
        private int partOffset = 0;
        @Option(
                name = { "-nr", "--num-revisions" },
                description = "Number of revisions to write per-partition (default: 10000)")
        private int numRevisions = 10000;
        @Option(name = { "-ro", "--revision-offset" }, description = "Revision offset to start from (default: 0)")
        private int revOffset = 0;
        @Option(name = {"--num-renders"}, description = "Number of revisions (overwrites)")
        private int numRenders = 1;
        @Option(name = "--concurrency", description = "Request concurrency (default: 10)")
        private int concurrency = 10;
        @Option(name = "--runs", description = "Number of runs to execute")
        private int runs = 1;

        @Override
        public void run() {
            if (this.help.showHelpIfRequested()) {
                return;
            }

            try (CassandraSession session = this.sessionBuilder().build()) {
                new Writer(
                        metrics,
                        session,
                        this.concurrency,
                        this.numPartitions,
                        this.partOffset,
                        this.numRevisions,
                        this.revOffset,
                        this.numRenders,
                        this.runs).execute();
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Command(name = "alt-write", description = "Write new revisions")
    public static class AltWrite extends Cmd {
        @Option(name = { "-np", "--num-partitions" }, description = "Number of partitions to write (default: 1000)")
        private int numPartitions = 1000;
        @Option(name = { "-po", "--partition-offset" }, description = "Partition offset to start from (default: 0)")
        private int partOffset = 0;
        @Option(
                name = { "-nr", "--num-revisions" },
                description = "Number of revisions to write per-partition (default: 10000)")
        private int numRevisions = 10000;
        @Option(name = { "-ro", "--revision-offset" }, description = "Revision offset to start from (default: 0)")
        private int revOffset = 0;
        @Option(name = {"--num-renders"}, description = "Number of revisions (overwrites)")
        private int numRenders = 1;
        @Option(name = "--concurrency", description = "Request concurrency (default: 10)")
        private int concurrency = 10;
        @Option(name = "--runs", description = "Number of runs to execute")
        private int runs = 1;

        @Override
        public void run() {
            if (this.help.showHelpIfRequested()) {
                return;
            }

            try (CassandraSession session = this.sessionBuilder().build()) {
                new AltWriter(
                        metrics,
                        session,
                        this.concurrency,
                        this.numPartitions,
                        this.partOffset,
                        this.numRevisions,
                        this.revOffset,
                        this.numRenders,
                        this.runs).execute();
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Command(name = "read", description = "Read previously written revisions")
    public static class Read extends Cmd {
        @Option(name = { "-np", "--num-partitions" }, description = "Number of partitions to read (default: 1000)")
        private int numPartitions = 1000;
        @Option(name = { "-po", "--partition-offset" }, description = "Partition offset to start from (default: 0)")
        private int partOffset = 0;
        @Option(name = "--concurrency", description = "Request concurrency (default: 10)")
        private int concurrency = 10;
        @Option(name = "--runs", description = "Number of runs to execute")
        private int runs = 1;

        @Override
        public void run() {
            if (this.help.showHelpIfRequested()) {
                return;
            }

            try (CassandraSession session = this.sessionBuilder().build()) {
                new Reader(
                        metrics,
                        session,
                        this.concurrency,
                        this.numPartitions,
                        this.partOffset,
                        this.runs).execute();
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Command(name = "alt-read", description = "Read previously written revisions")
    public static class AltRead extends Cmd {
        @Option(name = { "-np", "--num-partitions" }, description = "Number of partitions to read (default: 1000)")
        private int numPartitions = 1000;
        @Option(name = { "-po", "--partition-offset" }, description = "Partition offset to start from (default: 0)")
        private int partOffset = 0;
        @Option(name = "--concurrency", description = "Request concurrency (default: 10)")
        private int concurrency = 10;
        @Option(name = "--runs", description = "Number of runs to execute")
        private int runs = 1;

        @Override
        public void run() {
            if (this.help.showHelpIfRequested()) {
                return;
            }

            try (CassandraSession session = this.sessionBuilder().build()) {
                new AltReader(metrics, session, this.concurrency, this.numPartitions, this.partOffset, this.runs)
                        .execute();
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Command(name = "schema", description = "Output schema as CQL DDL")
    public static class Schema implements Runnable {

        @Override
        public void run() {
            try {
                System.out.println(new org.wikimedia.cassandra.Schema().get());
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Command(name = "get-alt-value", description = "Retrieve a value associated with a key")
    public static class GetAltValue extends Cmd {
        @Option(name = { "-k", "--key" }, description = "Key to retrieve")
        private String key;

        @Override
        public void run() {
            if (this.help.showHelpIfRequested()) {
                return;
            }

            try (CassandraSession session = this.sessionBuilder().build()) {
                AltValue v = new org.wikimedia.cassandra.GetAltValue(session).get(this.key);
                if (v != null) {
                    System.out.printf("revision=%d, timestamp=%s%n", v.getRevision(), new Date(v.getUnixTimestamp()));
                }
                else {
                    System.err.printf("value for %s not found%n", this.key);
                }
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }

        }

    }

    @Command(name = "help")
    public static class Help implements Runnable {
        @Inject
        private GlobalMetadata<Help> global;

        @Arguments(description = "Provides the name of the commands you want help for")
        private List<String> commands = new ArrayList<String>();

        @Override
        public void run() {
            try {
                com.github.rvesse.airline.help.Help.help(global, commands, true);
            }
            catch (IOException e) {
                System.err.println("Unable to output help: " + e.getMessage());
                e.printStackTrace(System.err);
            }
        }

    }

    private static final MetricRegistry metrics = new MetricRegistry();

    public static void main(String[] args) throws Exception {
        Slf4jReporter reporter = Slf4jReporter
                .forRegistry(metrics)
                .outputTo(LoggerFactory.getLogger(name(Main.class.getPackage().getName(), "metrics")))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.SECONDS);

        com.github.rvesse.airline.Cli<Runnable> cli = new com.github.rvesse.airline.Cli<>(Main.class);
        Runnable command = cli.parse(args);
        command.run();

        System.exit(0);
    }
}
