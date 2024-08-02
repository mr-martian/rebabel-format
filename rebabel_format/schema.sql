BEGIN;

CREATE TABLE metadata(
       schema_major INTEGER,
       schema_minor INTEGER
);
INSERT INTO metadata(schema_major, schema_minor) VALUES(1, 0);

CREATE TABLE units(
       id INTEGER PRIMARY KEY,
       type TEXT,
       created datetime DEFAULT (datetime('now')),
       modified datetime DEFAULT (datetime('now')),
       active bool DEFAULT 1
);

CREATE TABLE tiers(
       id INTEGER PRIMARY KEY,
       tier TEXT,
       feature TEXT,
       unittype TEXT,
       valuetype TEXT,
       CHECK(valuetype = 'int' OR valuetype = 'bool' OR
             valuetype = 'str' OR valuetype = 'ref')
);

CREATE TABLE int_features(
       id INTEGER PRIMARY KEY,
       unit INTEGER,
       feature INTEGER,
       value INTEGER,
       user TEXT,
       confidence INTEGER,
       date datetime DEFAULT (datetime('now')),
       probability REAL,
       active bool DEFAULT 1,
       FOREIGN KEY(unit) REFERENCES units(id),
       FOREIGN KEY(feature) REFERENCES tiers(id)
);

CREATE TABLE bool_features(
       id INTEGER PRIMARY KEY,
       unit INTEGER,
       feature INTEGER,
       value bool,
       user TEXT,
       confidence INTEGER,
       date datetime DEFAULT (datetime('now')),
       probability REAL,
       active bool DEFAULT 1,
       FOREIGN KEY(unit) REFERENCES units(id),
       FOREIGN KEY(feature) REFERENCES tiers(id)
);

CREATE TABLE str_features(
       id INTEGER PRIMARY KEY,
       unit INTEGER,
       feature INTEGER,
       value TEXT,
       user TEXT,
       confidence INTEGER,
       date datetime DEFAULT (datetime('now')),
       probability REAL,
       active bool DEFAULT 1,
       FOREIGN KEY(unit) REFERENCES units(id),
       FOREIGN KEY(feature) REFERENCES tiers(id)
);

-- `relations` is for parent-child connections,
-- `ref_features` is for all other types of references
CREATE TABLE ref_features(
       id INTEGER PRIMARY KEY,
       unit INTEGER,
       feature INTEGER,
       value INTEGER,
       user TEXT,
       confidence INTEGER,
       date datetime DEFAULT (datetime('now')),
       probability REAL,
       active bool DEFAULT 1,
       FOREIGN KEY(unit) REFERENCES units(id),
       FOREIGN KEY(feature) REFERENCES tiers(id),
       FOREIGN KEY(value) REFERENCES units(id)
);

-- types are redundant with units table, but it might simplify some
-- queries to duplicate that information (and it's not too much)
-- `isprimary` indicates whether this is the link that the child
-- would return if their parent (singular) is requested.
CREATE TABLE relations(
       id INTEGER PRIMARY KEY,
       parent INTEGER,
       parent_type TEXT,
       child INTEGER,
       child_type TEXT,
       isprimary bool,
       active bool DEFAULT 1,
       date datetime DEFAULT (datetime('now')),
       FOREIGN KEY(parent) REFERENCES units(id),
       FOREIGN KEY(child) REFERENCES units(id)
);

-- the type columns specify which tables the refence columns point into
-- "str", "bool", "int", and "ref" for `$1_features`
-- and "child" for `relations`
CREATE TABLE conflicts(
       id INTEGER PRIMARY KEY,
       value1 INTEGER, -- ref
       value1_type TEXT,
       value2 INTEGER, -- ref
       value2_type TEXT
);

CREATE TRIGGER new_int_feature BEFORE INSERT ON int_features
       BEGIN
        UPDATE int_features SET active = 0
               WHERE unit = NEW.unit AND feature = NEW.feature
                     AND date != NEW.date;
        UPDATE units SET modified = NEW.date WHERE id = NEW.unit;
       END;

CREATE TRIGGER new_bool_feature BEFORE INSERT ON bool_features
       BEGIN
        UPDATE bool_features SET active = 0
               WHERE unit = NEW.unit AND feature = NEW.feature
                     AND date != NEW.date;
        UPDATE units SET modified = NEW.date WHERE id = NEW.unit;
       END;

CREATE TRIGGER new_str_feature BEFORE INSERT ON str_features
       BEGIN
        UPDATE str_features SET active = 0
               WHERE unit = NEW.unit AND feature = NEW.feature
                     AND date != NEW.date;
        UPDATE units SET modified = NEW.date WHERE id = NEW.unit;
       END;

CREATE TRIGGER new_ref_feature BEFORE INSERT ON ref_features
       BEGIN
        UPDATE ref_features SET active = 0
               WHERE unit = NEW.unit AND feature = NEW.feature
                     AND date != NEW.date;
        UPDATE units SET modified = NEW.date WHERE id = NEW.unit;
       END;

COMMIT;
