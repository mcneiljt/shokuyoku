package com.mcneilio.shokuyoku.util;

import com.mcneilio.shokuyoku.model.EventError;
import com.mcneilio.shokuyoku.model.EventType;
import com.mcneilio.shokuyoku.model.EventTypeColumn;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import java.util.Properties;

public class DBUtil {

    public static SessionFactory getSessionFactory() {
        Properties cfg = new Properties();
        cfg.setProperty("hibernate.connection.url", System.getenv("DATABASE_URL"));
        cfg.setProperty("hibernate.hbm2ddl.auto", "update");

        return new Configuration().addProperties(cfg).
            addAnnotatedClass(EventError.class).
            addAnnotatedClass(EventType.class).
            addAnnotatedClass(EventTypeColumn.class).
            buildSessionFactory();
    }
}
