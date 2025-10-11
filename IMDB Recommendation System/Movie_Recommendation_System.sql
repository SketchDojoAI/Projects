/* =============================================================
   Recommendation System Script 
   By : Amit Pinchas
   -------------------------------------------------------------
   READ THIS FIRST – EXECUTION ORDER (MANDATORY):
   0) Ensure the base IMDB_IJS schema + data are already loaded
      (DDL imdb_Ijs.sql + base tables actors/movies/... available).
   1) Run THIS script until the CREATE TABLE statements finish
      for movies_recommendations and movies_recommendations_agg.
   2) Then load the GS records into those tables via:
         SOURCE 'movies_recommendations.sql';
         SOURCE 'movies_recommendations_agg.sql';
   3) Resume THIS script (indexes, gs_* subset, rules, union,
      and evaluation).
   -------------------------------------------------------------
   What this script creates:
   • GS tables definitions (empty) for compatibility.
   • Restricted GS subset (gs_*) with only GS-relevant rows.
   • Rules (Q1–Q5) as views + my_models_union.
   • Evaluation: TP/FP/FN + `Precision`/Recall/F1.
   Notes: use backticks for `rank` and `Precision`.
   Technical note: all tables are explicitly created with ENGINE=InnoDB to ensure
   support for foreign keys and transactions across environments. Charset is not
   specified explicitly, since only basic English text is stored and MySQL defaults
   (utf8mb4 in most setups) are sufficient.
   ============================================================= */

/* 0) Safety & schema */
USE IMDB_IJS;
SET sql_safe_updates = 0;

/* -------------------------------------------------------------
   1) CLASS GS TABLES – definitions (records are loaded via SOURCE)
   IMPORTANT: After creating these tables, run:
              SOURCE 'movies_recommendations.sql';
              SOURCE 'movies_recommendations_agg.sql';
              to actually populate them with the class data.
   ------------------------------------------------------------- */
CREATE TABLE IF NOT EXISTS movies_recommendations (
  base_movie_id         INT,
  recommended_movie_id  INT,
  recommendation        INT,
  suggested_by          VARCHAR(255),
  justification         VARCHAR(255) NOT NULL,
  comment               VARCHAR(255),
  PRIMARY KEY (base_movie_id, recommended_movie_id, suggested_by),
  CONSTRAINT CHK_recommendation CHECK (recommendation BETWEEN 1 AND 10),
  CONSTRAINT fk_mr_base  FOREIGN KEY (base_movie_id)        REFERENCES movies(id),
  CONSTRAINT fk_mr_rec   FOREIGN KEY (recommended_movie_id) REFERENCES movies(id)
);

/* _agg is provided as records (numbers) by the instructor. */
CREATE TABLE IF NOT EXISTS movies_recommendations_agg (
  base_movie_id        INT NOT NULL,
  recommended_movie_id INT NOT NULL,
  recommendation       DECIMAL(3,1) NOT NULL,
  recommendation_std   DECIMAL(5,3),
  suggested_by_num     INT,
  justifications_num   INT,
  PRIMARY KEY (base_movie_id, recommended_movie_id)
);


  
 /* ! (**If hadn't been done already**) Make sure to load 
       'movies_recommendations.sql' & 'movies_recommendations_agg.sql' to your MySQL-Workbench environment; */

/* -------------------------------------------------------------
   2) RESTRICTED GS SUBSET (gs_*) – only GS-relevant rows
   ------------------------------------------------------------- */

DROP TABLE IF EXISTS gs_movies_ids;
CREATE TABLE gs_movies_ids (
  id INT,
  PRIMARY KEY (id),
  FOREIGN KEY (id) REFERENCES movies(id)
);

INSERT INTO gs_movies_ids (id)
SELECT base_movie_id FROM movies_recommendations
UNION
SELECT recommended_movie_id FROM movies_recommendations;

DROP TABLE IF EXISTS gs_movies;
CREATE TABLE gs_movies (
  id   INT NOT NULL DEFAULT 0,
  name VARCHAR(100),
  year INT,
  `rank` DECIMAL(3,1),
  PRIMARY KEY (id),
  KEY movies_name (name)
) ENGINE=InnoDB;

INSERT INTO gs_movies (id, name, year, `rank`)
SELECT m.id, m.name, m.year, m.`rank`
FROM movies m
JOIN gs_movies_ids gsmi ON m.id = gsmi.id;

DROP TABLE IF EXISTS gs_roles;
CREATE TABLE gs_roles (
  actor_id INT NOT NULL,
  movie_id INT NOT NULL,
  role     VARCHAR(100) NOT NULL,
  PRIMARY KEY (actor_id, movie_id, role),
  KEY actor_id (actor_id),
  KEY movie_id (movie_id),
  CONSTRAINT gs_roles_ibfk_1 FOREIGN KEY (actor_id) REFERENCES actors(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT gs_roles_ibfk_2 FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;

INSERT INTO gs_roles (actor_id, movie_id, role)
SELECT r.actor_id, r.movie_id, r.role
FROM roles r
JOIN gs_movies_ids gsmi ON r.movie_id = gsmi.id;

DROP TABLE IF EXISTS gs_actors;
CREATE TABLE gs_actors (
  id         INT NOT NULL DEFAULT 0,
  first_name VARCHAR(100),
  last_name  VARCHAR(100),
  gender     CHAR(1),
  PRIMARY KEY (id),
  KEY actors_first_name (first_name),
  KEY actors_last_name  (last_name)
) ENGINE=InnoDB;

INSERT INTO gs_actors (id, first_name, last_name, gender)
SELECT DISTINCT a.id, a.first_name, a.last_name, a.gender
FROM actors a
JOIN gs_roles r ON a.id = r.actor_id;

DROP TABLE IF EXISTS gs_movies_directors;
CREATE TABLE gs_movies_directors (
  director_id INT NOT NULL,
  movie_id    INT NOT NULL,
  PRIMARY KEY (director_id, movie_id),
  KEY movies_directors_director_id (director_id),
  KEY movies_directors_movie_id    (movie_id),
  CONSTRAINT gs_movies_directors_ibfk_1 FOREIGN KEY (director_id) REFERENCES directors(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT gs_movies_directors_ibfk_2 FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;

INSERT INTO gs_movies_directors (director_id, movie_id)
SELECT md.director_id, md.movie_id
FROM movies_directors md
JOIN gs_movies_ids gsmi ON md.movie_id = gsmi.id;

DROP TABLE IF EXISTS gs_directors;
CREATE TABLE gs_directors (
  id         INT NOT NULL DEFAULT 0,
  first_name VARCHAR(100),
  last_name  VARCHAR(100),
  PRIMARY KEY (id),
  KEY directors_first_name (first_name),
  KEY directors_last_name  (last_name)
) ENGINE=InnoDB;

INSERT INTO gs_directors (id, first_name, last_name)
SELECT DISTINCT d.id, d.first_name, d.last_name
FROM directors d
JOIN gs_movies_directors gmd ON d.id = gmd.director_id;

DROP TABLE IF EXISTS gs_movies_genres;
CREATE TABLE gs_movies_genres (
  movie_id INT NOT NULL,
  genre    VARCHAR(100) NOT NULL,
  PRIMARY KEY (movie_id, genre),
  KEY movies_genres_movie_id (movie_id),
  CONSTRAINT gs_movies_genres_ibfk_1 FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB;

INSERT INTO gs_movies_genres (movie_id, genre)
SELECT mg.movie_id, mg.genre
FROM movies_genres mg
JOIN gs_movies_ids gsmi ON mg.movie_id = gsmi.id;

/* -------------------------------------------------------------
   3) EVALUATION UNIVERSE (within GS): movie pool
   ------------------------------------------------------------- */
CREATE OR REPLACE VIEW gs_movies_pool AS
SELECT id AS movie_id FROM gs_movies;

/* -------------------------------------------------------------
   4) RULES – Q1 to Q5 (with agreed improvements)
   ------------------------------------------------------------- */

-- Q1: Same director & same genre within 12 years, prolific >=3 (>=3 movies in that genre).
-- This threshold (3) ensures we only connect movies by directors who have made multiple contributions in the same genre, reducing noise

CREATE OR REPLACE VIEW q1_pairs AS
SELECT md_base.movie_id  AS base_id,
       md_other.movie_id AS rec_id
FROM gs_movies_directors AS md_base
JOIN gs_movies_genres AS mg_base ON mg_base.movie_id = md_base.movie_id
JOIN (
  SELECT md.director_id AS director_id, mg.genre AS genre
  FROM gs_movies_directors AS md
  JOIN gs_movies_genres AS mg ON mg.movie_id = md.movie_id
  GROUP BY md.director_id, mg.genre
  HAVING COUNT(*) >= 3
) AS dg ON dg.director_id = md_base.director_id AND dg.genre = mg_base.genre
JOIN gs_movies_directors AS md_other ON md_other.director_id = dg.director_id
JOIN gs_movies_genres AS mg_other    ON mg_other.movie_id = md_other.movie_id AND mg_other.genre = dg.genre
JOIN gs_movies AS ma ON ma.id = md_base.movie_id
JOIN gs_movies AS mb ON mb.id = md_other.movie_id
WHERE md_other.movie_id <> md_base.movie_id
  AND ABS(ma.year - mb.year) <= 12;

-- Q2: Shared actor + same genre
CREATE OR REPLACE VIEW q2_pairs AS
SELECT DISTINCT r1.movie_id AS base_id, r2.movie_id AS rec_id
FROM gs_roles AS r1
JOIN gs_roles AS r2 ON r1.actor_id = r2.actor_id
JOIN gs_movies_genres AS g1 ON g1.movie_id = r1.movie_id
JOIN gs_movies_genres AS g2 ON g2.movie_id = r2.movie_id AND g2.genre = g1.genre
WHERE r1.movie_id <> r2.movie_id;

-- Q3: Shared actor and shared director 
CREATE OR REPLACE VIEW q3_pairs AS
SELECT DISTINCT r1.movie_id AS base_id, r2.movie_id AS rec_id
FROM gs_roles AS r1
JOIN gs_movies_directors AS d1 ON d1.movie_id = r1.movie_id
JOIN gs_roles AS r2 ON r2.actor_id = r1.actor_id
JOIN gs_movies_directors AS d2 ON d2.movie_id = r2.movie_id
WHERE r1.movie_id <> r2.movie_id
  AND d1.director_id = d2.director_id;

-- Q4: Same genre + rank above genre average (+0.3)

-- !!! notice: Execute each part separately

-- 1st part 

CREATE OR REPLACE VIEW gs_genre_stats AS
SELECT g.genre AS genre, AVG(m.`rank`) AS avg_rank
FROM gs_movies_genres AS g JOIN gs_movies AS m ON m.id = g.movie_id
GROUP BY g.genre;

-- 2nd part

CREATE OR REPLACE VIEW q4_pairs AS
SELECT DISTINCT a.movie_id AS base_id, b.movie_id AS rec_id
FROM gs_movies_pool AS a
JOIN gs_movies_genres AS g1 ON g1.movie_id = a.movie_id
JOIN gs_movies_pool AS b ON b.movie_id <> a.movie_id
JOIN gs_movies_genres AS g2 ON g2.movie_id = b.movie_id AND g2.genre = g1.genre
JOIN gs_movies AS m2 ON m2.id = b.movie_id
JOIN gs_genre_stats AS s ON s.genre = g2.genre
WHERE m2.`rank` >= s.avg_rank + 0.3;

-- Q5: Same genre + within 8 years + rank >= genre average + at least 1 shared word in roles
-- Optimized: build role_words table with index, keep only words appearing in >=2 movies

DROP TABLE IF EXISTS role_words;
CREATE TABLE role_words AS
SELECT movie_id, LOWER(word) AS word
FROM (
  SELECT r.movie_id AS movie_id,
         SUBSTRING_INDEX(SUBSTRING_INDEX(r.role,' ',n.n), ' ', -1) AS word
  FROM gs_roles AS r
  JOIN (SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8) AS n
    ON CHAR_LENGTH(r.role) - CHAR_LENGTH(REPLACE(r.role,' ','')) >= n.n-1
) AS tokens
WHERE word REGEXP '^[[:alpha:]][[:alnum:]]{2,}$';  -- keep only valid tokens

-- Keep only words that appear in at least 2 different movies
DROP TABLE IF EXISTS role_words_filtered;
CREATE TABLE role_words_filtered AS
SELECT word, movie_id
FROM role_words
WHERE word IN (
  SELECT word FROM role_words GROUP BY word HAVING COUNT(DISTINCT movie_id) >= 2
);

CREATE INDEX idx_role_words_word ON role_words_filtered(word);

-- Build Q5 pairs from shared words across movies (filtered set)
CREATE OR REPLACE VIEW q5_pairs AS
SELECT DISTINCT LEAST(r1.movie_id, r2.movie_id) AS base_id,
                GREATEST(r1.movie_id, r2.movie_id) AS rec_id
FROM role_words_filtered AS r1
JOIN role_words_filtered AS r2 ON r1.word = r2.word
WHERE r1.movie_id < r2.movie_id
  AND EXISTS (
    SELECT 1 FROM gs_movies_genres AS g1
    JOIN gs_movies_genres AS g2 ON g1.genre = g2.genre
    WHERE g1.movie_id = r1.movie_id AND g2.movie_id = r2.movie_id
  )
  AND EXISTS (
    SELECT 1 FROM gs_movies AS m1
    JOIN gs_movies AS m2 ON 1=1
    JOIN gs_genre_stats AS s ON s.genre IN (SELECT genre FROM gs_movies_genres WHERE movie_id = r1.movie_id)
    WHERE m1.id = r1.movie_id AND m2.id = r2.movie_id
      AND ABS(m1.year - m2.year) <= 8
      AND m2.`rank` >= s.avg_rank
  );

/* -------------------------------------------------------------
   5) UNIFIED PREDICTOR (>=6 is positive for CM)
   ------------------------------------------------------------- */
CREATE OR REPLACE VIEW my_models_union AS
SELECT base_id AS source_id, rec_id AS rec_id, 'Q1'  AS rule, 10 AS score FROM q1_pairs
UNION
SELECT base_id AS source_id, rec_id AS rec_id, 'Q2'  AS rule,  9 AS score FROM q2_pairs
UNION
SELECT base_id AS source_id, rec_id AS rec_id, 'Q3'  AS rule,  9 AS score FROM q3_pairs
UNION
SELECT base_id AS source_id, rec_id AS rec_id, 'Q4'  AS rule,  8 AS score FROM q4_pairs
UNION
SELECT base_id AS source_id, rec_id AS rec_id, 'Q5'  AS rule,  7 AS score FROM q5_pairs;

/* -------------------------------------------------------------
   6) EVALUATION against class GS (movies_recommendations_agg)
   ------------------------------------------------------------- */
DROP TABLE IF EXISTS eval_hits;
CREATE TEMPORARY TABLE eval_hits AS
SELECT u.rule AS rule,
       u.source_id AS source_id,
       u.rec_id AS rec_id,
       (CASE WHEN gs.recommendation >= 6 THEN 1 ELSE 0 END) AS is_positive
FROM my_models_union AS u
JOIN movies_recommendations_agg AS gs
  ON gs.base_movie_id = u.source_id AND gs.recommended_movie_id = u.rec_id
  ORDER BY CAST(SUBSTRING(rule, 2) AS UNSIGNED);
  
  
  -- **The Creation of the table above (`eval_hits`) Takes time - please be paitent**

-- Per-rule metrics
DROP TABLE IF EXISTS rule_metrics;
CREATE TEMPORARY TABLE rule_metrics AS
SELECT rule AS rule,
       SUM(is_positive) AS TP,
       SUM(1 - is_positive) AS FP,
       (SELECT COUNT(*) FROM movies_recommendations_agg WHERE recommendation >= 6)
         - SUM(is_positive) AS FN
FROM eval_hits
GROUP BY rule
ORDER BY CAST(SUBSTRING(rule, 2) AS UNSIGNED);

-- Union-wide metrics (micro-average)
DROP TABLE IF EXISTS union_metrics;
CREATE TEMPORARY TABLE union_metrics AS
SELECT 'UNION_ALL' AS rule,
       SUM(is_positive) AS TP,
       SUM(1 - is_positive) AS FP,
       (SELECT COUNT(*) FROM movies_recommendations_agg WHERE recommendation >= 6)
         - SUM(is_positive) AS FN
FROM eval_hits;

-- Final results (note: backticks around `Precision`)
SELECT rule AS rule,
       TP AS TP, FP AS FP, FN AS FN,
       ROUND(TP / NULLIF(TP + FP, 0), 3) AS `Precision`,
       ROUND(TP / NULLIF(TP + FN, 0), 3) AS Recall,
       ROUND(2*TP / NULLIF(2*TP + FP + FN, 0), 3) AS F1
FROM (
  SELECT * FROM rule_metrics
  UNION ALL
  SELECT * FROM union_metrics
) AS x
ORDER BY (rule = 'UNION_ALL') DESC, rule;

SET sql_safe_updates = 1;
