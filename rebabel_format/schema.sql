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

CREATE TABLE features(
       unit INTEGER,
       feature INTEGER,
       value,
       user TEXT,
       confidence INTEGER,
       date datetime DEFAULT (datetime('now')),
       FOREIGN KEY(unit) REFERENCES units(id),
       FOREIGN KEY(feature) REFERENCES tiers(id),
       UNIQUE(unit, feature)
);
CREATE TABLE suggestions(
       unit INTEGER,
       feature INTEGER,
       value,
       date datetime DEFAULT (datetime('now')),
       probability REAL,
       FOREIGN KEY(unit) REFERENCES units(id),
       FOREIGN KEY(feature) REFERENCES tiers(id)
);
CREATE TABLE history(
       unit INTEGER,
       feature INTEGER,
       value,
       user TEXT,
       confidence INTEGER,
       start datetime DEFAULT (datetime('now')),
       end datetime DEFAULT (datetime('now')),
       FOREIGN KEY(unit) REFERENCES units(id),
       FOREIGN KEY(feature) REFERENCES tiers(id)
);
CREATE TRIGGER edit BEFORE UPDATE ON features
       BEGIN
        INSERT INTO history VALUES
               (OLD.unit, OLD.feature, OLD.value, OLD.user, OLD.confidence,
               OLD.date, NEW.date);
        UPDATE units SET modified = NEW.date WHERE id = NEW.unit;
       END;

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

COMMIT;
