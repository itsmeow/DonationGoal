package dev.itsmeow.donationgoal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.UUID;
import java.util.function.Consumer;

import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.Configuration;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.plugin.java.JavaPlugin;

import com.coloredcarrot.api.sidebar.Sidebar;
import com.coloredcarrot.api.sidebar.SidebarString;

public class DonationGoal extends JavaPlugin implements CommandExecutor, Listener {

    private static Connection db;
    private static String db_host, db_database, db_username, db_password, db_prefix;
    private static int db_port;
    private static boolean db_useSSL;

    private static Statement statement;

    private static String table;
    private static String midnight_table;
    private static double goal;

    private static Thread sql_thread = new Thread("DonationGoalSQL") {
        @Override
        public void run() {
            while(!this.isInterrupted()) {
                if(!queue.empty()) {
                    queue.pop().run();
                }
            }
        }
    };
    private static Stack<Runnable> queue = new Stack<Runnable>();

    private static int progressBarWidth;
    private static String progressBarBackColor;
    private static String progressBarFillColor;
    private static String donationText;

    private static Sidebar sidebar;

    @Override
    public void onEnable() {
        this.getCommand("donationgoal").setExecutor(this);
        Configuration cfg = this.getConfig();
        cfg.addDefault("sql.host", "127.0.0.1");
        cfg.addDefault("sql.port", 3306);
        cfg.addDefault("sql.database", "donationgoal");
        cfg.addDefault("sql.username", "");
        cfg.addDefault("sql.password", "");
        cfg.addDefault("sql.prefix", "dg_");
        cfg.addDefault("sql.use_ssl", true);
        cfg.addDefault("goal", 15.0D);
        cfg.addDefault("progress_bar_width", 12);
        cfg.addDefault("progress_bar_back_color", "&c");
        cfg.addDefault("progress_bar_fill_color", "&b");
        cfg.addDefault("donation_text", "&cDonate: &e/buy");
        cfg.options().copyDefaults(true);
        this.saveConfig();
        this.loadConfig();
        sidebar = new Sidebar(ChatColor.translateAlternateColorCodes('&', "&b&l&nDonation Goal"), this, 120, new SidebarString("Loading..."), new SidebarString("Loading..."), new SidebarString("Loading..."), new SidebarString("Loading..."));
        sql_thread.start();
        sql(() -> loadSQL(() -> {
            this.resetIfNeeded(this::updateSidebar);
            this.scheduleMidnightTask();
            this.scheduleMinuteTask();
        }));
        Bukkit.getPluginManager().registerEvents(this, this);
    }

    @EventHandler
    public void onPlayerJoin(PlayerJoinEvent event) {
        updateSidebar();
    }

    private void scheduleMinuteTask() {
        getServer().getScheduler().runTaskLater(this, () -> {
            updateSidebar();
            // re-schedule
            scheduleMinuteTask();
        }, 1200);

    }

    private void scheduleMidnightTask() {
        ZonedDateTime nextTime = ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()).plusDays(1L).truncatedTo(ChronoUnit.DAYS);
        long delay = Duration.between(ZonedDateTime.now(), nextTime).getSeconds() * 20;
        // run daily at midnight
        getServer().getScheduler().runTaskLater(this, () -> {
            // run task
            resetIfNeeded();
            // re-schedule
            scheduleMidnightTask();
        }, delay);
    }

    private void updateSidebar() {
        execQuery("SELECT SUM(amount) FROM " + table + ";", result -> {
            result.next();
            double donations = result.getDouble(1);
            String color = (donations > goal ? "&5" : (donations == goal ? "&a" : "&c"));
            final String line =  color + "$" + donations + " &7out of &a$" + goal;
            double rawfraction = donations / goal;
            final String line1 = color + Math.floor(rawfraction * 100D) + "%";
            double fraction = Math.min(rawfraction, 1D);
            String line3temp = progressBarFillColor;
            for(int i = 0; i < Math.floor(fraction * ((double) progressBarWidth)); i++) {
                line3temp += "■";
            }
            line3temp += progressBarBackColor;
            for(int i = (int) Math.floor(fraction * ((double) progressBarWidth)) + 1; i <= ((double) progressBarWidth); i++) {
                line3temp += "■";
            }
            final String line2 = line3temp;
            final String line3 = donationText;
            result.close();
            sync(() -> {
                setLine(0, line);
                setLine(1, line1);
                setLine(2, line2);
                setLine(3, line3);
                sidebar.update();
                Bukkit.getOnlinePlayers().stream().forEach(p -> {
                    if(!p.hasPermission("donationgoal.display")) {
                        sidebar.hideFrom(p);
                    } else {
                        sidebar.showTo(p);
                    }
                });
                sidebar.update();
            });
        });
    }

    private static void setLine(int line, String text) {
        sidebar.getEntries().get(line).getVariations().set(0, text);
    }

    private void resetIfNeeded() {
        resetIfNeeded(() -> {
        });
    }

    private void resetIfNeeded(Runnable callback) {
        System.out.println("Database reset task running, checking if a new month has begun.");
        // query database for if row exists
        execQuery("SELECT count(*) FROM " + midnight_table + " WHERE id=0", result -> {
            // if row exists
            if(result.next() && result.getInt(1) > 0) {
                result.close();
                // get the last reset time
                execQuery("SELECT last_time FROM " + midnight_table + " WHERE id=0", r -> {
                    r.next();
                    String timeStr = r.getString("last_time");
                    System.out.println("Got last reset time from database: " + timeStr);
                    ZonedDateTime time = ZonedDateTime.parse(timeStr);
                    // check if reset time was before this month
                    if(time.isBefore(ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()).withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS))) {
                        // clear!
                        resetGoal();
                        // set reset time to now
                        execUpdate("UPDATE " + midnight_table + " SET last_time='" + ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()).toString() + "' WHERE id=0;");
                    } else {
                        System.out.println("Time is in current month, no reset taking place.");
                    }
                });
            } else {
                result.close();
                // no row, clear and insert new row!
                System.out.println("No database entry found for last month. Inserting one and clearing database!");
                resetGoal();
                execUpdate("INSERT INTO " + midnight_table + " (id, last_time) VALUES (0, '" + ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()).toString() + "');");
            }
            callback.run();
        });
    }

    private void resetGoal() {
        System.out.println("Clearing database for the new month! Goal reset!");
        execUpdate("DELETE FROM " + table + ";");
    }

    private void loadSQL() {
        try {
            openConnection();
            statement = db.createStatement();
            statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + db_database + ";");
            statement.executeUpdate("USE " + db_database + ";");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + table + "( id MEDIUMINT NOT NULL AUTO_INCREMENT PRIMARY KEY, uuid varchar(255) NOT NULL, amount double(10, 2) NOT NULL);");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + midnight_table + " ( id MEDIUMINT NOT NULL PRIMARY KEY, last_time varchar(255));");
        } catch(ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private void loadSQL(Runnable callback) {
        loadSQL();
        callback.run();
    }

    private void loadConfig() {
        Configuration cfg = this.getConfig();
        db_database = cfg.getString("sql.database");
        db_host = cfg.getString("sql.host");
        db_username = cfg.getString("sql.username");
        db_password = cfg.getString("sql.password");
        db_prefix = cfg.getString("sql.prefix");
        db_port = cfg.getInt("sql.port");
        db_useSSL = cfg.getBoolean("sql.use_ssl");
        goal = cfg.getDouble("goal");
        progressBarWidth = cfg.getInt("progress_bar_width");
        progressBarBackColor = cfg.getString("progress_bar_back_color");
        progressBarFillColor = cfg.getString("progress_bar_fill_color");
        donationText = cfg.getString("donation_text");
        table = db_prefix + "donations";
        midnight_table = db_prefix + "monthly";
    }

    @Override
    public void onDisable() {
        disconnectSQL();
        sql_thread.interrupt();
    }

    private static void disconnectSQL() {
        try {
            if(statement != null) {
                statement.close();
                statement = null;
            }
            if(db != null) {
                db.close();
                db = null;
            }
        } catch(SQLException e) {
            e.printStackTrace();
        }
    }

    public void sql(Runnable run) {
        queue.add(run);
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if(sender.hasPermission("donationgoal.manipulate")) {
            if(args.length == 1) {
                if(args[0].equals("reload")) {
                    this.loadConfig();
                    sql(() -> {
                        disconnectSQL();
                        loadSQL(() -> {
                            this.resetIfNeeded(this::updateSidebar);
                        });
                    });
                    sender.sendMessage("Sucessfully reloaded!");
                }
            }
            if(args.length > 0) {
                if(args.length == 3) {
                    if(args[0].equals("adddonation")) {
                        UUID uuid = UUID.fromString(args[1]);
                        double amount = Double.parseDouble(args[2]);
                        final String exec = "INSERT INTO " + table + " (uuid, amount) VALUES ('" + uuid + "', " + amount + ");";
                        sender.sendMessage("Executing: " + exec);
                        execUpdate(exec, this::updateSidebar);
                    }
                    if(args[0].equals("removedonation")) {
                        UUID uuid = UUID.fromString(args[1]);
                        double amount = Double.parseDouble(args[2]);
                        final String exec = "DELETE FROM " + table + " WHERE uuid='" + uuid + "' AND amount=" + amount + " LIMIT 1;";
                        sender.sendMessage("Executing: " + exec);
                        execUpdate(exec, this::updateSidebar);
                    }
                } else if(args[0].equals("querydb")) {
                    UUID uuid = null;
                    double amount = 0;
                    if(args.length == 2) {
                        uuid = UUID.fromString(args[1]);
                    }
                    if(args.length == 3) {
                        amount = Double.parseDouble(args[2]);
                    }
                    final String exec = "SELECT * FROM " + table + (args.length == 2 ? (" WHERE uuid='" + uuid + "'" + (args.length == 3 ? " AND amount=" + amount : "")) : "") + ";";
                    sender.sendMessage("Executing: " + exec);
                    execQuery(exec, result -> {
                        List<String> messages = new ArrayList<String>();
                        while(result.next()) {
                            messages.add("UUID: " + result.getString("uuid"));
                            messages.add("Amount: " + result.getDouble("amount"));
                        }
                        result.close();
                        this.syncMessage(sender, messages.toArray(new String[0]));
                    });
                } else if(args[0].equals("cleardb")) {
                    final String exec = "DELETE FROM " + table + ";";
                    sender.sendMessage("Executing: " + exec);
                    execUpdate(exec, this::updateSidebar);
                } else if(args[0].equals("checkreset")) {
                    resetIfNeeded(this::updateSidebar);
                    sender.sendMessage("Checking if database reset is needed. See console for results.");
                } else if(args[0].equals("updatesidebar")) {
                    updateSidebar();
                    sender.sendMessage("Updating sidebar!");
                }
            }
        } else {
            sender.sendMessage("You do not have permission to use this command!");
        }
        return true;
    }

    public void syncMessage(CommandSender sender, String... messages) {
        Bukkit.getScheduler().runTask(this, () -> {
            for(String message : messages) {
                sender.sendMessage(message);
            }
        });
    }

    public void sync(Runnable run) {
        Bukkit.getScheduler().runTask(this, run);
    }

    public void execQuery(String query, SqlConsumer<ResultSet> callback) {
        sql(() -> {
            try {
                callback.accept(statement.executeQuery(query));
            } catch(SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @FunctionalInterface
    private interface SqlConsumer<T> extends Consumer<T> {

        @Override
        default void accept(final T elem) {
            try {
                acceptThrows(elem);
            } catch(SQLException e) {
                e.printStackTrace();
            }
        }

        public void acceptThrows(final T elem) throws SQLException;
    }

    public void execUpdate(String update) {
        sql(() -> {
            try {
                statement.executeUpdate(update);
            } catch(SQLException e) {
                e.printStackTrace();
            }
        });
    }

    public void execUpdate(String update, Runnable callback) {
        sql(() -> {
            try {
                statement.executeUpdate(update);
                callback.run();
            } catch(SQLException e) {
                e.printStackTrace();
            }
        });
    }

    public void openConnection() throws SQLException, ClassNotFoundException {
        if(db != null && !db.isClosed()) {
            return;
        }

        synchronized(this) {
            if(db != null && !db.isClosed()) {
                return;
            }
            Class.forName("com.mysql.jdbc.Driver");
            db = DriverManager.getConnection("jdbc:mysql://" + db_host + ":" + db_port + "/" + db_database + "?useSSL=" + db_useSSL, db_username, db_password);
        }
    }

}
