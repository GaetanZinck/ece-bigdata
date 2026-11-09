"""
Exécute les labs Trino 2 (silver -> gold) et 3 (star schema) contre TON cluster Trino,
capture les vraies sorties et génère rapport_trino.pdf + rapport_trino.md, avec les
réponses aux questions de compréhension calculées à partir de tes résultats.

Usage (dans le dossier du repo, après l'étape 4) :
    uv add trino pandas reportlab
    uv run python rapport_trino.py --nom "Prénom Nom"

Options :
    --sans-exercices   ne pas exécuter les exercices du lab 3
    --mock             test hors cluster (sorties factices, NE PAS rendre)
"""
import argparse
import datetime as dt
import os
import re
import subprocess
import textwrap

import pandas as pd

CAT = "lakehouse"
EX3_ORDER_ID = "00000000-0000-4000-8000-000000000001"  # commande ajoutée par l'exercice 3
GOLD_TABLES = [
    "orders_by_state_month", "top_products", "user_activity", "dim_user", "dim_product",
    "dim_date", "fact_orders", "orders_by_state_month_part", "cohorts",
]

# --------------------------------------------------------------------------------------
# Connexion et journal du rapport
# --------------------------------------------------------------------------------------
args = None
conn = None
report = []  # liste de (type, contenu...)


def connect():
    global conn
    import trino
    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", os.environ.get("KUBERNETES_NAMESPACE", "student")),
        http_scheme="http",
        catalog=CAT,
    )


def raw_sql(statement):
    """Helper du lab : exécute et récupère toujours le résultat."""
    if args.mock:
        return pd.DataFrame({"mock": [1]})
    cur = conn.cursor()
    cur.execute(statement)
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=cols)


def H1(t): report.append(("h1", t))
def H2(t): report.append(("h2", t))
def H3(t): report.append(("h3", t))
def P(t): report.append(("p", textwrap.dedent(t).strip()))
def CODE(t): report.append(("code", textwrap.dedent(t).strip("\n")))
def OUT(t): report.append(("out", t))
def QA(q, a): report.append(("qa", q, textwrap.dedent(a).strip()))


def fmt(df, max_rows=25):
    if df is None:
        return "(pas de résultat)"
    if df.empty:
        return "(0 ligne)" if len(df.columns) else "(instruction exécutée, pas de résultat)"
    with pd.option_context("display.width", 200, "display.max_columns", 20,
                           "display.max_colwidth", 70):
        return df.to_string(max_rows=max_rows)


def run(statement, show=True, max_rows=25, label=None):
    """Exécute une requête, l'ajoute au rapport avec sa sortie réelle (ou l'erreur)."""
    statement = textwrap.dedent(statement).strip()
    if show:
        CODE(label or f'sql("""\n{statement}\n""")')
    try:
        df = raw_sql(statement)
        if show:
            OUT(fmt(df, max_rows))
        print(".", end="", flush=True)
        return df
    except Exception as e:  # on continue : l'erreur réelle figure dans le rapport
        msg = f"ERREUR : {type(e).__name__}: {str(e)[:600]}"
        if show:
            OUT(msg)
        print("\n" + msg)
        return None


def run_text(statement, max_lines=80):
    """Pour EXPLAIN : sortie texte sur une seule cellule."""
    statement = textwrap.dedent(statement).strip()
    CODE(f'print(sql("""\n{statement}\n""").iloc[0, 0])')
    df = run(statement, show=False)
    text = str(df.iloc[0, 0]) if df is not None and not df.empty else "(échec de la requête)"
    lines = text.splitlines()
    shown = "\n".join(lines[:max_lines])
    if len(lines) > max_lines:
        shown += f"\n... ({len(lines) - max_lines} lignes supplémentaires coupées)"
    OUT(shown)
    return text


def v(df, r=0, c=0, default=None):
    """Valeur d'une cellule, ou default si la requête a échoué."""
    try:
        x = df.iloc[r][c] if isinstance(c, str) else df.iloc[r, c]
        return default if pd.isna(x) else x
    except Exception:
        return default


def joincol(df, col, default="?"):
    """Valeurs d'une colonne jointes par des virgules, sans planter si la requête a échoué."""
    try:
        return ", ".join(str(x) for x in df[col].dropna())
    except Exception:
        return default


def check(label, a, b):
    ok = a is not None and b is not None and a == b
    CODE(f"assert {label}")
    OUT(f"{'OK' if ok else 'ÉCHEC'} : {a} == {b}")
    return ok


def shell(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=60).stdout
    except Exception as e:
        return f"(commande impossible : {e})"


# --------------------------------------------------------------------------------------
# Nettoyage pour pouvoir relancer le script
# --------------------------------------------------------------------------------------
def cleanup():
    if args.mock:
        return
    for t in GOLD_TABLES:
        run(f"DROP TABLE IF EXISTS {CAT}.gold.{t}", show=False)
    n = v(run(f"SELECT count(*) FROM {CAT}.silver.orders WHERE order_id = '{EX3_ORDER_ID}'",
              show=False), default=0)
    if n:
        run(f"DELETE FROM {CAT}.silver.orders WHERE order_id = '{EX3_ORDER_ID}'", show=False)


# --------------------------------------------------------------------------------------
# LAB 2 : silver -> gold
# --------------------------------------------------------------------------------------
def lab2():
    H1("Lab 2 : Silver to gold avec Trino")

    H2("Environnement")
    P("""Connexion au coordinateur Trino depuis Python avec le client `trino`. Le paramètre
    catalog="lakehouse" définit le catalogue par défaut de la session.""")
    CODE("""
    conn = trino.dbapi.connect(host="trino", port=8080, user=os.environ["TRINO_USER"],
                               http_scheme="http", catalog="lakehouse")
    """)
    version = run("SELECT version()")
    P("Effet du catalogue par défaut : les noms non qualifiés sont résolus dans `lakehouse`.")
    run("SHOW SCHEMAS")
    unq = run("SELECT count(*) AS orders FROM silver.orders")
    QA("Que change le paramètre catalog= de connect() dans les instructions suivantes ?",
       f"""Il fixe le catalogue par défaut de la session. Un nom à deux niveaux comme
       `silver.orders` est résolu en `lakehouse.silver.orders`. La requête non qualifiée
       ci-dessus renvoie {v(unq)} lignes, exactement comme la forme complète. De même,
       `SHOW SCHEMAS` sans FROM liste les schémas de `lakehouse`. Les noms complets
       `catalogue.schéma.table` ne changent pas. Le schéma reste obligatoire, car aucun
       `schema=` n'est passé à connect().""")

    H2("Catalogues, schémas et tables")
    run("SHOW CATALOGS")
    run(f"SHOW SCHEMAS FROM {CAT}")
    run(f"SHOW TABLES FROM {CAT}.silver")
    d_orders = run(f"DESCRIBE {CAT}.silver.orders")
    run(f"DESCRIBE {CAT}.silver.users")

    def coltype(df, col):
        try:
            return df.loc[df["Column"] == col, "Type"].iloc[0]
        except Exception:
            return "(inconnu : DESCRIBE a échoué)"

    t_at, t_id = coltype(d_orders, "ordered_at"), coltype(d_orders, "order_id")
    QA("Quel type Trino porte ordered_at ? Garde-t-il un fuseau horaire ?",
       f"""Trino rapporte `{t_at}`. Spark a écrit un `TimestampType`, stocké en Iceberg comme
       `timestamptz`, c'est-à-dire un instant normalisé en UTC. Trino l'expose donc
       « with time zone » et affiche les valeurs en UTC (+00:00). L'instant est conservé.
       Le décalage d'origine de la source n'est pas conservé : toutes les valeurs sont
       ramenées à UTC.""")
    QA("Spark a déclaré order_id en string faute de type UUID. Que rapporte Trino ?",
       f"""Trino rapporte `{t_id}`. Le fichier de métadonnées Iceberg déclare une colonne
       `string`, et Trino ne peut pas deviner qu'elle contient des UUID. Il la traite donc
       comme du texte : aucune validation de format, une comparaison et un stockage plus
       coûteux qu'un UUID binaire de 16 octets. Pour obtenir un vrai type `uuid`, il
       faudrait déclarer explicitement la colonne en `uuid` dans Iceberg à la création de
       la table.""")

    H2("Interroger la couche silver")
    n_orders = run(f"SELECT count(*) AS orders FROM {CAT}.silver.orders")
    n_users = run(f"SELECT count(*) AS users FROM {CAT}.silver.users")
    states = run(f"""
        SELECT state, count(*) AS users, count_if(city IS NULL) AS missing_city
        FROM {CAT}.silver.users
        GROUP BY state
        ORDER BY users DESC
        LIMIT 10
    """)
    orphans = run(f"""
        SELECT count(*) AS orphan_orders
        FROM {CAT}.silver.orders AS o
        LEFT JOIN {CAT}.silver.users AS u ON u.user_id = o.user_id
        WHERE u.user_id IS NULL
    """)
    no, nu, norph = v(n_orders), v(n_users), v(orphans)
    military = ""
    try:
        m = states[states["missing_city"] > 0]
        military = ", ".join(f"{r.state} ({r.missing_city}/{r.users})" for r in m.itertuples())
    except Exception:
        pass
    QA("Expliquer la sortie des requêtes exécutées.",
       f"""La couche silver contient {no} commandes et {nu} utilisateurs. Le texte du lab
       annonce 5000 commandes, mais le générateur (graine 42) produit 2829 commandes pour
       50 utilisateurs, chiffre confirmé par le lab DuckDB et par la taille du fichier
       bronze (331 568 octets). Le dédoublonnage de Spark n'a donc rien retiré.
       La répartition par état montre que les « états » sans ville ({military or 'aucun'})
       sont des codes militaires américains (AA, AE, AP) : les adresses de type
       « Unit ... Box ... DPO AP 09617 » n'ont pas de ville, et la regex laisse alors
       `city` à NULL, ce qui est le comportement voulu.
       La requête d'orphelins renvoie {norph} : chaque commande a un utilisateur
       correspondant, donc une jointure interne ne perdra aucune ligne.""")
    QA("Quel test du lab dbt la requête d'orphelins remplace-t-elle ? Que faut-il écrire "
       "pour l'exécuter à chaque rafraîchissement ?",
       """Elle remplace le test `relationships` de dbt (stg_orders.user_id doit exister dans
       stg_users.user_id), qui vérifie l'intégrité référentielle. Pour l'exécuter à chaque
       rafraîchissement, il faut en faire un contrôle bloquant dans le pipeline : soit le
       test `relationships` déclaré dans le schema.yml d'un projet dbt-trino et lancé par
       `dbt build`, soit une étape du pipeline qui exécute la requête et échoue si le
       résultat est supérieur à 0 (par exemple un `assert` Python, ou une tâche de
       l'orchestrateur), placée avant la construction de la couche gold.""")
    QA("Trino a répondu sans job à soumettre ni session à créer. Qu'est-ce que cela dit "
       "sur le moment où choisir Trino plutôt que Spark ?",
       """Trino est un service MPP qui tourne en permanence : le coordinateur et les workers
       sont déjà démarrés, avec les catalogues configurés côté serveur. Une requête SQL
       s'exécute donc en quelques centaines de millisecondes, sans démarrer de JVM ni
       télécharger de dépendances. Trino est le bon choix pour les requêtes interactives,
       ad hoc et BI (latence faible, nombreux utilisateurs simultanés). Spark convient mieux
       aux traitements batch lourds et longs, aux transformations programmatiques
       (Python, ML) et aux jobs qui doivent survivre à la perte d'un nœud.""")

    H2("Métadonnées Iceberg")
    snaps = run(f"""
        SELECT snapshot_id, committed_at, operation,
               element_at(summary, 'engine-name') AS engine
        FROM {CAT}.silver."orders$snapshots"
        ORDER BY committed_at
    """)
    files = run(f"""
        SELECT file_path, file_format, record_count, file_size_in_bytes
        FROM {CAT}.silver."orders$files"
    """)
    run(f'SELECT * FROM {CAT}.silver."orders$history"')
    bucket = os.environ.get("LAB_BUCKET_NAME", os.environ.get("KUBERNETES_NAMESPACE", ""))
    ls_cmd = f'aws s3 --profile default ls "s3://{bucket}/warehouse/silver/orders/" --recursive'
    CODE(ls_cmd)
    ls_out = shell(ls_cmd).strip() if not args.mock else "(mock)"
    OUT(ls_out or "(vide ou commande indisponible)")
    n_snap = len(snaps) if snaps is not None else "?"
    engines = ", ".join(sorted(set(joincol(snaps, "engine", "").split(", ")) - {""}))
    ops = joincol(snaps, "operation")
    n_files = len(files) if files is not None else "?"
    n_s3_data = sum(1 for l in ls_out.splitlines() if "/data/" in l)
    n_s3_all = len(ls_out.splitlines())
    QA("Combien de snapshots silver.orders a-t-elle ? Quel moteur les a écrits ?",
       f"""{n_snap} snapshot(s), opération(s) : {ops}. La colonne `engine`, lue dans le
       résumé du snapshot, vaut : {engines or "non renseigné"}. Ils ont donc été écrits par
       Spark lors du lab Spark (writeTo...createOrReplace()). L'enregistrement dans Polaris
       (register_table) ne crée aucun snapshot : il ajoute seulement un pointeur vers le
       fichier de métadonnées existant. Le lab en montre 3 parce que le lab Iceberg
       (module 08) ajoute deux snapshots « append ».""")
    QA("Lister les fichiers avec aws s3 ls et comparer avec $files. Lequel n'a pas besoin "
       "d'accéder au bucket (en listing), et pourquoi ?",
       f"""`$files` renvoie {n_files} fichier(s) de données. `aws s3 ls` liste {n_s3_all}
       objets, dont {n_s3_data} sous data/. Il montre aussi les fichiers de métadonnées
       (metadata.json, manifestes avro, version-hint) et, après des réécritures, des
       fichiers de données qui ne sont plus référencés. `$files` n'a pas besoin de lister
       le bucket : Trino demande à Polaris l'emplacement du metadata.json courant, puis lit
       la manifest list et les manifestes, qui contiennent la liste exacte des fichiers du
       snapshot courant, avec leur nombre de lignes et leur taille. C'est le principe
       d'Iceberg : la liste des fichiers est dans les métadonnées, pas dans un LIST S3
       (coûteux et éventuellement incohérent sur un gros bucket).""")

    H2("Plan de requête et exécution")
    plan = run_text(f"""
        EXPLAIN (TYPE DISTRIBUTED)
        SELECT u.state, count(*) AS orders
        FROM {CAT}.silver.orders AS o
        JOIN {CAT}.silver.users  AS u ON u.user_id = o.user_id
        GROUP BY u.state
    """)
    n_frag = len(re.findall(r"^Fragment \d+", plan, re.M))
    n_scan = len(re.findall(r"TableScan\[", plan))
    n_remote = len(re.findall(r"RemoteSource\[", plan))
    dist = sorted(set(re.findall(r"distribution\s*=\s*(\w+)", plan)))
    P(f"""Lecture du plan : {n_frag} fragments (un par stage), {n_scan} opérateurs
    TableScan (un par table), {n_remote} RemoteSource (échanges REMOTE entre fragments),
    distribution de la jointure : {', '.join(dist) or 'non trouvée'}. Avec 50 utilisateurs
    contre {no} commandes, l'optimiseur à base de coûts diffuse la petite table (REPLICATED,
    broadcast) au lieu de repartitionner les deux côtés sur user_id (PARTITIONED).""")
    run_text(f"""
        EXPLAIN ANALYZE
        SELECT u.state, count(*) AS orders
        FROM {CAT}.silver.orders AS o
        JOIN {CAT}.silver.users  AS u ON u.user_id = o.user_id
        GROUP BY u.state
    """, max_lines=90)
    q = run("""
        SELECT query_id, state,
               date_diff('millisecond', created, coalesce("end", current_timestamp)) AS elapsed_ms,
               substr(query, 1, 60) AS query
        FROM system.runtime.queries
        ORDER BY created DESC
        LIMIT 5
    """)
    if q is None:
        P("system.runtime.queries est refusé par le contrôle d'accès du cluster.")
    P("""Interface web : le chart est déployé avec `ingress.enabled: false`, le coordinateur
    n'est donc pas exposé hors du cluster. L'interface /ui n'est accessible que par
    `kubectl port-forward svc/trino 8080:8080`, ce qui ne permet pas de l'ouvrir depuis un
    navigateur dans l'environnement Onyxia. La table system.runtime.queries ci-dessus donne
    le même historique.""")

    H2("Couche gold")
    run(f"CREATE SCHEMA IF NOT EXISTS {CAT}.gold")
    run(f"SHOW SCHEMAS FROM {CAT}")
    H3("Commandes par état et par mois")
    run(f"""
        CREATE TABLE {CAT}.gold.orders_by_state_month AS
        SELECT
            u.state,
            date_trunc('month', o.ordered_at) AS month,
            count(*)                          AS orders,
            sum(o.quantity)                   AS units,
            count(DISTINCT o.user_id)         AS buyers
        FROM {CAT}.silver.orders AS o
        JOIN {CAT}.silver.users  AS u ON u.user_id = o.user_id
        GROUP BY 1, 2
    """)
    run(f"SELECT * FROM {CAT}.gold.orders_by_state_month ORDER BY month, state LIMIT 10")
    H3("Produits les plus vendus")
    run(f"""
        CREATE TABLE {CAT}.gold.top_products AS
        SELECT
            product,
            count(*)      AS orders,
            sum(quantity) AS units,
            rank() OVER (ORDER BY sum(quantity) DESC) AS units_rank
        FROM {CAT}.silver.orders
        GROUP BY product
    """)
    run(f"SELECT * FROM {CAT}.gold.top_products ORDER BY units_rank LIMIT 10")
    H3("Activité de chaque utilisateur")
    run(f"""
        CREATE TABLE {CAT}.gold.user_activity AS
        SELECT
            u.user_id,
            u.state,
            date_diff('year', u.birthdate, current_date) AS age,
            count(o.order_id)            AS orders,
            coalesce(sum(o.quantity), 0) AS units,
            min(o.ordered_at)            AS first_order_at,
            max(o.ordered_at)            AS last_order_at
        FROM {CAT}.silver.users AS u
        LEFT JOIN {CAT}.silver.orders AS o ON o.user_id = u.user_id
        GROUP BY u.user_id, u.state, u.birthdate
    """)
    run(f"SELECT * FROM {CAT}.gold.user_activity ORDER BY orders DESC LIMIT 10")
    run(f"SHOW TABLES FROM {CAT}.gold")
    gfiles = run(f"""
        SELECT file_path, record_count, file_size_in_bytes
        FROM {CAT}.gold."top_products$files"
    """)
    zero = v(run(f"SELECT count_if(orders = 0) AS users_without_order "
                 f"FROM {CAT}.gold.user_activity"))
    gpath = str(v(gfiles, 0, "file_path", "(inconnu)"))
    gdir = gpath.split("/data/")[0] if "/data/" in gpath else gpath
    QA("Pourquoi user_activity utilise un LEFT JOIN et orders_by_state_month une jointure "
       "interne ? Laquelle dépend du résultat de la requête d'orphelins ?",
       f"""`user_activity` décrit chaque utilisateur, y compris ceux qui n'ont jamais
       commandé : le LEFT JOIN depuis users les conserve, avec orders = 0 et units = 0
       grâce à coalesce. Ici, {zero} utilisateur(s) sans commande, car le générateur
       donne au moins une commande à chacun ; le LEFT JOIN protège le modèle pour des
       données futures. `orders_by_state_month` agrège des commandes par l'état de
       l'acheteur : une commande sans utilisateur n'a pas d'état, et la jointure interne
       la supprime silencieusement. C'est cette table qui dépend de la requête d'orphelins.
       Avec {norph} orphelin(s), elle ne perd rien, et la validation
       sum(units) = sum(silver.quantity) le confirme plus bas.""")
    QA("current_date fait changer age chaque jour. Qu'est-ce que cela implique pour le "
       "rafraîchissement de la table ?",
       """La table est figée au jour du CREATE TABLE AS SELECT : dès le lendemain, `age`
       peut être faux pour les utilisateurs dont c'est l'anniversaire. Le résultat dépend
       de la date d'exécution, donc il n'est pas reproductible (deux exécutions de la même
       requête sur les mêmes données donnent des tables différentes, et le time travel
       montre l'âge de l'époque). Il faut soit rafraîchir la table au moins une fois par
       jour, soit stocker `birthdate` et calculer l'âge au moment de la lecture, par
       exemple dans une vue.""")
    QA("Où vivent les fichiers de données des tables gold, et qui a décidé de leur "
       "emplacement ?",
       f"""Dans le bucket, sous `{gdir}/data/`. Trino n'a pas choisi de chemin : à la
       création de la table, il demande au catalogue l'emplacement du namespace `gold`.
       Polaris le dérive de la `default-base-location` du catalogue
       (s3://<bucket>/warehouse/) suivie de gold/. Trino y ajoute le nom de la table,
       suffixé d'un identifiant unique (option iceberg.unique-table-location, activée par
       défaut). Polaris vérifie que cet emplacement est dans les « allowed locations » du
       catalogue.""")

    H2("Validation")
    s_units = v(run(f"SELECT sum(quantity) AS v FROM {CAT}.silver.orders"))
    g_units = v(run(f"SELECT sum(units) AS v FROM {CAT}.gold.orders_by_state_month"))
    p_units = v(run(f"SELECT sum(units) AS v FROM {CAT}.gold.top_products"))
    check("silver_units == gold_units", s_units, g_units)
    check("silver_units == product_units", s_units, p_units)
    s_users = v(run(f"SELECT count(*) AS v FROM {CAT}.silver.users"))
    g_users = v(run(f"SELECT count(*) AS v FROM {CAT}.gold.user_activity"))
    s_ord = v(run(f"SELECT count(*) AS v FROM {CAT}.silver.orders"))
    g_ord = v(run(f"SELECT sum(orders) AS v FROM {CAT}.gold.user_activity"))
    check("silver_users == gold_users", s_users, g_users)
    check("silver_orders == gold_orders", s_ord, g_ord)

    H2("Rafraîchir une table gold")
    run(f"DELETE FROM {CAT}.gold.top_products")
    run(f"""
        INSERT INTO {CAT}.gold.top_products
        SELECT product, count(*), sum(quantity),
               rank() OVER (ORDER BY sum(quantity) DESC)
        FROM {CAT}.silver.orders
        GROUP BY product
    """)
    tsnaps = run(f"""
        SELECT snapshot_id, committed_at, operation
        FROM {CAT}.gold."top_products$snapshots"
        ORDER BY committed_at
    """)
    first = v(tsnaps, 0, "snapshot_id")
    run(f"SELECT count(*) AS products FROM {CAT}.gold.top_products FOR VERSION AS OF {first}")
    run(f"SELECT count(*) AS products FROM {CAT}.gold.top_products")
    ops = joincol(tsnaps, "operation")
    QA("Combien de snapshots la table a-t-elle après le rafraîchissement, et quelle est "
       "l'opération de chacun ?",
       f"""{len(tsnaps) if tsnaps is not None else '?'} snapshots, dans l'ordre : {ops}.
       Le CREATE TABLE AS SELECT crée le premier snapshot (append des données initiales).
       Le DELETE commit un snapshot qui retire toutes les lignes : comme il supprime des
       fichiers entiers, Trino fait une suppression au niveau des métadonnées, sans
       réécrire de données. L'INSERT commit un snapshot append avec les nouvelles lignes.
       Chaque instruction est une transaction autonome, donc un snapshot. Le time travel
       sur le premier snapshot retrouve les 6 produits initiaux.""")
    QA("Entre le DELETE et l'INSERT, un dashboard lit la table. Que voit-il ? Que changerait "
       "un MERGE ?",
       """Il voit une table vide : le snapshot courant est celui du DELETE, qui est déjà
       commité. Le dashboard affiche donc zéro produit jusqu'à la fin de l'INSERT. Un MERGE
       applique les mises à jour, les insertions et les suppressions dans une seule
       transaction, donc un seul snapshot : le lecteur voit soit l'ancienne version
       complète, soit la nouvelle, jamais d'état intermédiaire. Un
       `CREATE OR REPLACE TABLE ... AS SELECT` obtient le même effet pour un remplacement
       complet.""")

    H2("Comparaison avec Spark")
    QA("Que faut-il à chaque moteur pour exécuter la requête top_products, en plus de la "
       "requête elle-même ?",
       """Trino : un cluster déjà déployé (une fois pour toutes, par Helm) avec le catalogue
       `lakehouse` configuré côté serveur (URI Polaris, identifiant du principal, endpoint
       et clés S3), plus un client léger (`trino` en Python) et les paramètres de connexion.
       L'utilisateur ne manipule aucun identifiant et n'a pas de job à soumettre.
       Spark : une SparkSession à créer (démarrage d'une JVM driver), le package
       iceberg-spark-runtime téléchargé depuis Maven, les clés S3 (S3A,
       TemporaryAWSCredentialsProvider) et l'identifiant Polaris configurés dans la session
       par chaque utilisateur, et un job (shell interactif, spark-submit ou SparkApplication
       sur Kubernetes) dont il faut gérer les ressources et la fin.""")
    QA("Trino ou Spark : dashboard rafraîchi chaque minute sur gold ; transformation "
       "nocturne de 10 To de bronze ; data scientist qui explore silver avec une requête ?",
       """Dashboard chaque minute : Trino, car le service est toujours démarré, la latence
       est faible, la concurrence aussi, et la couche gold est petite.
       10 To la nuit : Spark, qui tolère les pannes (relance des tâches et des stages
       perdus), traite de gros shuffles en débordant sur disque, et peut monter en charge
       ponctuellement puis libérer les ressources.
       Exploration par un data scientist : Trino pour des requêtes SQL rapides et
       itératives. Spark devient pertinent si l'exploration continue en Python (DataFrame,
       pandas, ML) sur de gros volumes.""")
    QA("Trino échoue sur toute la requête si un worker est perdu (sans fault-tolerant "
       "execution). Pourquoi est-ce acceptable ici, et pas pour 10 To ?",
       """Les requêtes de ce lab durent quelques secondes sur quelques milliers de lignes :
       les relancer en cas d'échec ne coûte presque rien, et la probabilité de perdre un
       nœud pendant ce temps est négligeable. Une transformation de 10 To dure des heures
       sur de nombreux nœuds. La probabilité d'une panne croît avec la durée et le nombre de
       nœuds, et tout recommencer coûte des heures de calcul. Il faut alors pouvoir relancer
       seulement les tâches perdues : c'est ce que fait Spark grâce à son lignage, et ce que
       fait Trino avec retry-policy=TASK et un exchange manager qui écrit les résultats
       intermédiaires sur un stockage externe.""")


# --------------------------------------------------------------------------------------
# LAB 3 : star schema
# --------------------------------------------------------------------------------------
def lab3():
    H1("Lab 3 : Star schema")
    P("Même connexion Python que le lab 2 (la session reste ouverte).")

    H2("Dimensions")
    run(f"""
        CREATE TABLE {CAT}.gold.dim_user AS
        SELECT
            row_number() OVER (ORDER BY user_id) AS user_key,
            user_id, username, name, sex, birthdate, city, state, zip_code
        FROM {CAT}.silver.users
    """)
    run(f"""
        CREATE TABLE {CAT}.gold.dim_product AS
        SELECT row_number() OVER (ORDER BY product) AS product_key, product
        FROM (SELECT DISTINCT product FROM {CAT}.silver.orders)
    """)
    days = run(f"""
        SELECT CAST(min(ordered_at) AS date) AS first_day,
               CAST(max(ordered_at) AS date) AS last_day
        FROM {CAT}.silver.orders
    """)
    first_day, last_day = v(days, 0, 0, "2020-01-01"), v(days, 0, 1, "2020-12-31")
    run(f"""
        CREATE TABLE {CAT}.gold.dim_date AS
        SELECT
            CAST(date_format(CAST(d AS timestamp), '%Y%m%d') AS integer) AS date_key,
            d                                       AS full_date,
            year(d)                                 AS year,
            quarter(d)                              AS quarter,
            month(d)                                AS month,
            date_format(CAST(d AS timestamp), '%M') AS month_name,
            day_of_week(d)                          AS day_of_week,
            day_of_week(d) IN (6, 7)                AS is_weekend
        FROM UNNEST(sequence(DATE '{first_day}', DATE '{last_day}', INTERVAL '1' DAY)) AS t(d)
    """)

    H2("Table de faits")
    run(f"""
        CREATE TABLE {CAT}.gold.fact_orders AS
        SELECT
            o.order_id,
            u.user_key,
            p.product_key,
            CAST(date_format(o.ordered_at, '%Y%m%d') AS integer) AS date_key,
            o.ordered_at,
            o.quantity
        FROM {CAT}.silver.orders AS o
        LEFT JOIN {CAT}.gold.dim_user    AS u ON u.user_id = o.user_id
        LEFT JOIN {CAT}.gold.dim_product AS p ON p.product = o.product
    """)
    counts = run(f"""
        SELECT 'dim_user' AS table_name, count(*) AS row_count FROM {CAT}.gold.dim_user
        UNION ALL SELECT 'dim_product', count(*) FROM {CAT}.gold.dim_product
        UNION ALL SELECT 'dim_date', count(*) FROM {CAT}.gold.dim_date
        UNION ALL SELECT 'fact_orders', count(*) FROM {CAT}.gold.fact_orders
    """)
    P("""Le lab annonce 5000 lignes dans fact_orders. Avec le jeu de données réellement
    généré, on attend autant de lignes que silver.orders (2829).""")

    H2("Interroger l'étoile")
    run(f"""
        SELECT d.year, u.state, p.product,
               count(*)        AS orders,
               sum(f.quantity) AS units
        FROM {CAT}.gold.fact_orders AS f
        JOIN {CAT}.gold.dim_date    AS d ON d.date_key    = f.date_key
        JOIN {CAT}.gold.dim_user    AS u ON u.user_key    = f.user_key
        JOIN {CAT}.gold.dim_product AS p ON p.product_key = f.product_key
        GROUP BY d.year, u.state, p.product
        ORDER BY units DESC
        LIMIT 10
    """)
    run(f"""
        SELECT u.state, d.year, d.month,
               count(*)                   AS orders,
               sum(f.quantity)            AS units,
               count(DISTINCT f.user_key) AS buyers
        FROM {CAT}.gold.fact_orders AS f
        JOIN {CAT}.gold.dim_date    AS d ON d.date_key = f.date_key
        JOIN {CAT}.gold.dim_user    AS u ON u.user_key = f.user_key
        GROUP BY u.state, d.year, d.month
        ORDER BY d.year, d.month, u.state
        LIMIT 10
    """)
    P("Contrôle exhaustif (au-delà des 10 premières lignes) : différence symétrique entre "
      "l'agrégat gold et sa reconstruction depuis l'étoile.")
    diff = run(f"""
        WITH agg AS (
            SELECT state, year(month) AS year, month(month) AS month, orders, units, buyers
            FROM {CAT}.gold.orders_by_state_month
        ), star AS (
            SELECT u.state, d.year, d.month, count(*) AS orders, sum(f.quantity) AS units,
                   count(DISTINCT f.user_key) AS buyers
            FROM {CAT}.gold.fact_orders AS f
            JOIN {CAT}.gold.dim_date AS d ON d.date_key = f.date_key
            JOIN {CAT}.gold.dim_user AS u ON u.user_key = f.user_key
            GROUP BY u.state, d.year, d.month
        )
        SELECT (SELECT count(*) FROM (SELECT * FROM agg EXCEPT SELECT * FROM star)) AS only_in_agg,
               (SELECT count(*) FROM (SELECT * FROM star EXCEPT SELECT * FROM agg)) AS only_in_star
    """)

    H2("Validation")
    s_ord = v(run(f"SELECT count(*) AS v FROM {CAT}.silver.orders"))
    f_ord = v(run(f"SELECT count(*) AS v FROM {CAT}.gold.fact_orders"))
    s_units = v(run(f"SELECT sum(quantity) AS v FROM {CAT}.silver.orders"))
    f_units = v(run(f"SELECT sum(quantity) AS v FROM {CAT}.gold.fact_orders"))
    check("silver_orders == fact_orders", s_ord, f_ord)
    check("silver_units == fact_units", s_units, f_units)
    unmatched = v(run(f"""
        SELECT count(*) AS v
        FROM {CAT}.gold.fact_orders AS f
        LEFT JOIN {CAT}.gold.dim_user    AS u ON u.user_key    = f.user_key
        LEFT JOIN {CAT}.gold.dim_product AS p ON p.product_key = f.product_key
        LEFT JOIN {CAT}.gold.dim_date    AS d ON d.date_key    = f.date_key
        WHERE u.user_key IS NULL OR p.product_key IS NULL OR d.date_key IS NULL
    """))
    check("unmatched == 0", unmatched, 0)

    H3("Contrôles supplémentaires (réponse à la question 1)")
    recon = run(f"""
        SELECT count(*) AS mismatches
        FROM {CAT}.gold.fact_orders AS f
        JOIN {CAT}.silver.orders AS o ON o.order_id = f.order_id
        LEFT JOIN {CAT}.gold.dim_user    AS u ON u.user_key    = f.user_key
        LEFT JOIN {CAT}.gold.dim_product AS p ON p.product_key = f.product_key
        LEFT JOIN {CAT}.gold.dim_date    AS d ON d.date_key    = f.date_key
        WHERE u.user_id   IS DISTINCT FROM o.user_id
           OR p.product   IS DISTINCT FROM o.product
           OR d.full_date IS DISTINCT FROM CAST(o.ordered_at AS date)
           OR f.quantity  IS DISTINCT FROM o.quantity
    """)
    uniq = run(f"""
        SELECT
          (SELECT count(*) - count(DISTINCT order_id) FROM {CAT}.gold.fact_orders) AS dup_order_id,
          (SELECT count(*) - count(DISTINCT user_id)  FROM {CAT}.gold.dim_user)    AS dup_user_id,
          (SELECT count(*) - count(DISTINCT product)  FROM {CAT}.gold.dim_product) AS dup_product
    """)

    QA("Ces contrôles suffisent-ils à garantir que chaque commande est correctement "
       "représentée ? Exemple de défaut non détecté, et contrôles à ajouter.",
       f"""Non. Les trois assertions vérifient des totaux (nombre de lignes, somme des
       quantités) et la présence d'une clé, pas la justesse de chaque ligne. Un défaut qui
       conserve les totaux passe inaperçu : par exemple deux commandes dont les user_key
       sont intervertis, un product_key qui pointe vers le mauvais produit (clés décalées
       après une reconstruction de dim_product sans reconstruire les faits), un date_key
       calculé dans le mauvais fuseau qui place une commande proche de minuit sur la
       veille, ou une commande dupliquée compensée par une commande perdue de même
       quantité.
       Contrôles ajoutés et exécutés ci-dessus : (1) réconciliation ligne à ligne par
       order_id entre la table de faits et silver, en comparant user_id, product, date et
       quantity via les dimensions : {v(recon)} écart(s) ; (2) unicité de order_id dans les
       faits et des clés naturelles dans les dimensions : doublons = {v(uniq, 0, 0)} /
       {v(uniq, 0, 1)} / {v(uniq, 0, 2)}. On peut aussi comparer des agrégats par dimension
       (unités par produit, par état, par jour) entre silver et l'étoile.""")
    QA("Quel est le grain de fact_orders ? De dim_date ?",
       """fact_orders : une ligne par commande (order_id), soit le grain de silver.orders.
       dim_date : une ligne par jour calendaire entre la première et la dernière commande,
       identifiée par date_key = AAAAMMJJ.""")
    QA("Quelle assertion échoue si silver.users contient deux fois le même user_id ? "
       "Laquelle si une commande n'a pas de produit (NULL) ?",
       """user_id en double : dim_user contient deux lignes, avec deux user_key, pour le même
       user_id. Le LEFT JOIN sur user_id duplique alors toutes les commandes de cet
       utilisateur : fact_orders a plus de lignes que silver.orders, et
       `silver_orders == fact_orders` échoue (ainsi que `silver_units == fact_units`).
       Produit NULL : SELECT DISTINCT garde le NULL, donc dim_product reçoit une ligne avec
       product NULL. Mais l'égalité `p.product = o.product` n'est jamais vraie avec NULL :
       la commande garde product_key NULL. Les comptes et les sommes restent justes ; c'est
       l'assertion `unmatched == 0` qui échoue.""")
    QA("Clés de substitution générées par row_number() : que devient user_key si dim_user "
       "est reconstruite après l'ajout d'un utilisateur ? Conséquence pour fact_orders ?",
       """row_number() numérote par ordre de user_id. Un nouvel utilisateur dont l'UUID se
       place au milieu décale de 1 la clé de tous les utilisateurs suivants. Les clés ne
       sont donc pas stables d'une reconstruction à l'autre. Si fact_orders n'est pas
       reconstruite en même temps, ses user_key désignent les mauvais utilisateurs, sans
       aucune erreur, et l'assertion `unmatched` ne le voit pas. Il faut soit toujours
       reconstruire les faits avec les dimensions, soit générer des clés stables :
       attribuer max(user_key) + n aux seuls nouveaux utilisateurs avec un MERGE, ou
       dériver la clé d'un hash de la clé naturelle.""")
    QA("Si un utilisateur déménage, quel état montrent ses commandes passées ? Que faudrait-il "
       "pour garder l'historique ?",
       """dim_user écrase les attributs (Slowly Changing Dimension de type 1) : toutes les
       commandes, y compris les anciennes, affichent le nouvel état, et l'historique par
       état est réécrit. Pour garder l'historique, il faut une SCD de type 2 : une ligne
       par version de l'utilisateur, avec sa propre user_key, des colonnes valid_from,
       valid_to et is_current. La table de faits rattache chaque commande à la version
       valide à la date de commande (user_id = ... AND ordered_at >= valid_from AND
       ordered_at < valid_to).""")
    QA("Comparer orders_by_state_month et la requête en étoile qui la reconstruit. Quelles "
       "questions chacune traite-t-elle sans nouvelle table, et que coûte l'étoile ?",
       f"""Les deux donnent les mêmes chiffres (différence symétrique : only_in_agg =
       {v(diff, 0, 0)}, only_in_star = {v(diff, 0, 1)}). L'agrégat ne répond qu'à sa question
       (état x mois), mais il est petit, précalculé et lu instantanément par un outil BI.
       L'étoile répond à toute combinaison de dimensions sans nouvelle table : par produit,
       année, trimestre, week-end, ville, âge, etc. En contrepartie, chaque requête paie à
       l'exécution les jointures entre la table de faits et les dimensions, puis
       l'agrégation sur toutes les lignes de faits. C'est négligeable ici, mais cela croît
       avec le volume. Elle demande aussi de connaître le modèle pour écrire les jointures.
       En pratique, on combine les deux : l'étoile comme socle, et des agrégats pour les
       tableaux de bord les plus consultés.""")


# --------------------------------------------------------------------------------------
# Exercices du lab 3
# --------------------------------------------------------------------------------------
def exercices():
    H1("Lab 3 : Exercices")

    H2("Exercice 1 : table partitionnée par mois")
    run(f"""
        CREATE TABLE {CAT}.gold.orders_by_state_month_part
        WITH (partitioning = ARRAY['month(month)']) AS
        SELECT * FROM {CAT}.gold.orders_by_state_month
    """)
    fp = run(f"""
        SELECT partition, file_path, record_count
        FROM {CAT}.gold."orders_by_state_month_part$files"
    """)
    fu = run(f"""
        SELECT file_path, record_count
        FROM {CAT}.gold."orders_by_state_month$files"
    """)
    m0 = str(v(run(f"""
        SELECT date_format(min(month), '%Y-%m-%d %H:%i:%s') AS m
        FROM {CAT}.gold.orders_by_state_month
    """), default="2020-01-01 00:00:00"))
    flt = f"WHERE month = TIMESTAMP '{m0} UTC'"
    a1 = run_text(f"EXPLAIN ANALYZE SELECT sum(units) FROM {CAT}.gold.orders_by_state_month {flt}",
                  max_lines=40)
    a2 = run_text(f"EXPLAIN ANALYZE SELECT sum(units) FROM {CAT}.gold.orders_by_state_month_part {flt}",
                  max_lines=40)

    def scan_input(text):
        m = re.search(r"ScanFilter.*?Input:\s*([\d,]+)\s*rows", text, re.S) or \
            re.search(r"TableScan.*?Input:\s*([\d,]+)\s*rows", text, re.S)
        return m.group(1) if m else "?"

    P(f"""Résultat : la table non partitionnée a {len(fu) if fu is not None else '?'} fichier(s),
    la table partitionnée {len(fp) if fp is not None else '?'} (au moins un par mois). Lignes
    lues par le scan pour le mois {m0[:7]} : {scan_input(a1)} sans partitionnement, contre
    {scan_input(a2)} avec. Le filtre sur `month` correspond à la transformation de partition
    month(month) : Trino élague les fichiers des autres mois à partir des statistiques de
    partition des manifestes, avant de lire les données.""")

    H2("Exercice 2 : cohortes par décennie de naissance")
    run(f"""
        CREATE TABLE {CAT}.gold.cohorts AS
        SELECT
            (year(u.birthdate) / 10) * 10      AS birth_decade,
            count(DISTINCT u.user_id)          AS users,
            count(o.order_id)                  AS orders,
            round(avg(o.quantity), 2)          AS avg_units_per_order
        FROM {CAT}.silver.users AS u
        LEFT JOIN {CAT}.silver.orders AS o ON o.user_id = u.user_id
        GROUP BY 1
    """)
    run(f"SELECT * FROM {CAT}.gold.cohorts ORDER BY birth_decade")
    P("""count(DISTINCT user_id) est nécessaire, car la jointure multiplie chaque utilisateur
    par son nombre de commandes. avg(quantity) ignore les NULL des utilisateurs sans
    commande et donne directement la moyenne d'unités par commande.""")

    H2("Exercice 4 : OPTIMIZE")
    P("Exécuté avant l'exercice 3, qui modifie silver.")
    sb = run(f'SELECT snapshot_id, committed_at, operation FROM {CAT}.gold."top_products$snapshots" ORDER BY committed_at')
    fb = run(f'SELECT file_path, record_count, file_size_in_bytes FROM {CAT}.gold."top_products$files"')
    run(f"ALTER TABLE {CAT}.gold.top_products EXECUTE optimize")
    sa = run(f'SELECT snapshot_id, committed_at, operation FROM {CAT}.gold."top_products$snapshots" ORDER BY committed_at')
    fa = run(f'SELECT file_path, record_count, file_size_in_bytes FROM {CAT}.gold."top_products$files"')
    P(f"""Avant : {len(sb) if sb is not None else '?'} snapshots, {len(fb) if fb is not None else '?'}
    fichier(s) dans le snapshot courant. Après : {len(sa) if sa is not None else '?'} snapshots,
    {len(fa) if fa is not None else '?'} fichier(s). OPTIMIZE réécrit les petits fichiers (et
    applique les fichiers de suppression éventuels) en fichiers plus gros, puis commit un
    nouveau snapshot d'opération « replace » : les données logiques ne changent pas, seule
    leur organisation physique change. Si la table n'a qu'un seul petit fichier, rien n'est
    réécrit et aucun snapshot n'est ajouté. Les anciens fichiers restent dans le bucket
    tant que les snapshots qui les référencent ne sont pas expirés (expire_snapshots puis
    remove_orphan_files).""")

    H2("Exercice 3 : INSERT dans silver, MERGE dans gold, time travel")
    u = run(f"SELECT user_id, state FROM {CAT}.silver.users ORDER BY user_id LIMIT 1")
    uid, ustate = v(u, 0, "user_id", "?"), v(u, 0, "state", "?")
    before = v(run(f"""
        SELECT snapshot_id FROM {CAT}.gold."orders_by_state_month$snapshots"
        ORDER BY committed_at DESC LIMIT 1
    """))
    run(f"""
        INSERT INTO {CAT}.silver.orders (order_id, user_id, ordered_at, quantity, product)
        VALUES ('{EX3_ORDER_ID}', '{uid}', TIMESTAMP '2020-01-15 12:00:00.000000 UTC', 3, 'croissant')
    """)
    run(f"""
        MERGE INTO {CAT}.gold.orders_by_state_month AS t
        USING (
            SELECT u.state, date_trunc('month', o.ordered_at) AS month,
                   count(*) AS orders, sum(o.quantity) AS units,
                   count(DISTINCT o.user_id) AS buyers
            FROM {CAT}.silver.orders AS o
            JOIN {CAT}.silver.users  AS u ON u.user_id = o.user_id
            GROUP BY 1, 2
        ) AS s
        ON t.state = s.state AND t.month = s.month
        WHEN MATCHED AND (t.orders <> s.orders OR t.units <> s.units OR t.buyers <> s.buyers)
            THEN UPDATE SET orders = s.orders, units = s.units, buyers = s.buyers
        WHEN NOT MATCHED
            THEN INSERT (state, month, orders, units, buyers)
                 VALUES (s.state, s.month, s.orders, s.units, s.buyers)
    """)
    w = f"WHERE state = '{ustate}' AND month = TIMESTAMP '2020-01-01 00:00:00 UTC'"
    run(f"SELECT * FROM {CAT}.gold.orders_by_state_month FOR VERSION AS OF {before} {w}")
    run(f"SELECT * FROM {CAT}.gold.orders_by_state_month {w}")
    run(f'SELECT snapshot_id, committed_at, operation FROM {CAT}.gold."orders_by_state_month$snapshots" ORDER BY committed_at')
    P(f"""La commande ajoutée pour l'utilisateur de l'état {ustate} en janvier 2020 augmente
    orders de 1 et units de 3 sur une seule ligne. Le MERGE ne met à jour que les lignes
    dont les agrégats ont changé, en un seul snapshot, donc de façon atomique pour les
    lecteurs. `FOR VERSION AS OF` relit la version précédente à partir de son snapshot_id.
    Remarque : la commande reste dans silver.orders ; ce script la supprime au début de sa
    prochaine exécution.""")

    H2("Exercice 5 : même agrégation depuis Spark")
    P("""Non exécuté par ce script, car il demande une session Spark connectée à Polaris
    (cf. lab Polaris). Dans cette session :
    spark.sql("SELECT u.state, date_trunc('month', o.ordered_at) m, count(*) FROM
    polaris.silver.orders o JOIN polaris.silver.users u USING (user_id) GROUP BY 1, 2").count()
    doit donner le même nombre de lignes que gold.orders_by_state_month, car les deux
    moteurs lisent les mêmes fichiers via le même catalogue.""")


# --------------------------------------------------------------------------------------
# Sorties : Markdown + PDF
# --------------------------------------------------------------------------------------
def write_md(path, title, meta):
    out = [f"# {title}", "", meta, ""]
    for item in report:
        k = item[0]
        if k == "h1": out += [f"# {item[1]}", ""]
        elif k == "h2": out += [f"## {item[1]}", ""]
        elif k == "h3": out += [f"### {item[1]}", ""]
        elif k == "p": out += [item[1], ""]
        elif k == "code": out += ["```python", item[1], "```", ""]
        elif k == "out": out += ["```text", item[1], "```", ""]
        elif k == "qa": out += [f"**Question : {item[1]}**", "", item[2], ""]
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(out))


def write_pdf(path, title, meta):
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_JUSTIFY
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.platypus import (KeepTogether, Paragraph, Preformatted, SimpleDocTemplate,
                                    Spacer, Table, TableStyle)
    from xml.sax.saxutils import escape

    base, mono = "Helvetica", "Courier"
    for reg, m in [("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf")]:
        if os.path.exists(reg) and os.path.exists(m):
            pdfmetrics.registerFont(TTFont("DV", reg))
            pdfmetrics.registerFont(TTFont("DVM", m))
            base, mono = "DV", "DVM"

    def clean(s):
        s = str(s)
        if base == "Helvetica":  # polices standard : latin-1 uniquement
            s = s.encode("cp1252", "replace").decode("cp1252")
        return s

    ss = getSampleStyleSheet()
    st = {
        "title": ParagraphStyle("t", parent=ss["Title"], fontName=base, fontSize=18),
        "h1": ParagraphStyle("h1", parent=ss["Heading1"], fontName=base, fontSize=15,
                             textColor=colors.HexColor("#1f3b6f"), spaceBefore=10),
        "h2": ParagraphStyle("h2", parent=ss["Heading2"], fontName=base, fontSize=12.5,
                             textColor=colors.HexColor("#1f3b6f")),
        "h3": ParagraphStyle("h3", parent=ss["Heading3"], fontName=base, fontSize=11),
        "p": ParagraphStyle("p", parent=ss["BodyText"], fontName=base, fontSize=9.5,
                            leading=13, alignment=TA_JUSTIFY),
        "q": ParagraphStyle("q", parent=ss["BodyText"], fontName=base, fontSize=9.5,
                            leading=13, textColor=colors.HexColor("#7a3e00")),
        "code": ParagraphStyle("c", fontName=mono, fontSize=7.3, leading=9,
                               textColor=colors.HexColor("#0b3d91")),
        "out": ParagraphStyle("o", fontName=mono, fontSize=7, leading=8.6),
    }

    def box(flow, bg):
        t = Table([[flow]], colWidths=[17.6 * cm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(bg)),
            ("BOX", (0, 0), (-1, -1), 0.4, colors.HexColor("#c8c8c8")),
            ("LEFTPADDING", (0, 0), (-1, -1), 5), ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        return t

    def para(text, style):
        txt = escape(clean(text)).replace("\n", " ")
        txt = re.sub(r"`([^`]+)`", rf'<font name="{mono}">\1</font>', txt)
        return Paragraph(txt, style)

    def pre_chunks(text, style, bg, label):
        lines = clean(text).splitlines() or [""]
        out = []
        for i in range(0, len(lines), 60):  # coupe les longues sorties sur plusieurs pages
            chunk = "\n".join(lines[i:i + 60])
            out.append(box(Preformatted(chunk, style, maxLineLength=118), bg))
        out[0] = KeepTogether([Paragraph(f'<font size="7" color="#888888">{label}</font>',
                                         st["p"]), out[0]])
        return out + [Spacer(1, 4)]

    story = [Paragraph(escape(clean(title)), st["title"]), para(meta, st["p"]), Spacer(1, 8)]
    for item in report:
        k = item[0]
        if k in ("h1", "h2", "h3"):
            story.append(Paragraph(escape(clean(item[1])), st[k]))
        elif k == "p":
            story += [para(item[1], st["p"]), Spacer(1, 4)]
        elif k == "code":
            story += pre_chunks(item[1], st["code"], "#eef3fb", "Commande")
        elif k == "out":
            story += pre_chunks(item[1], st["out"], "#f6f6f6", "Sortie")
        elif k == "qa":
            story += [Spacer(1, 3), para("Question : " + item[1], st["q"]),
                      para(item[2], st["p"]), Spacer(1, 6)]

    def footer(c, d):
        c.saveState()
        c.setFont(base, 7.5)
        c.setFillColor(colors.grey)
        c.drawRightString(A4[0] - 1.5 * cm, 1 * cm, f"page {d.page}")
        c.restoreState()

    doc = SimpleDocTemplate(path, pagesize=A4, leftMargin=1.5 * cm, rightMargin=1.5 * cm,
                            topMargin=1.4 * cm, bottomMargin=1.6 * cm, title=title)
    doc.build(story, onFirstPage=footer, onLaterPages=footer)


# --------------------------------------------------------------------------------------
def main():
    global args
    ap = argparse.ArgumentParser()
    ap.add_argument("--nom", default="")
    ap.add_argument("--sans-exercices", action="store_true")
    ap.add_argument("--mock", action="store_true")
    ap.add_argument("--sortie", default="rapport_trino")
    args = ap.parse_args()

    if not args.mock:
        connect()
    print("Exécution des requêtes (un point par requête)")
    cleanup()
    lab2()
    lab3()
    if not args.sans_exercices:
        exercices()
    if conn is not None:
        conn.close()

    title = "Rapport de TP : Trino, labs 2 et 3"
    meta = (f"{args.nom + ' | ' if args.nom else ''}"
            f"Namespace : {os.environ.get('KUBERNETES_NAMESPACE', '?')} | "
            f"Exécuté le {dt.datetime.now():%d/%m/%Y à %H:%M} | "
            f"Toutes les sorties ci-dessous sont celles de cette exécution.")
    if args.mock:
        meta = "SORTIES FACTICES (--mock) : NE PAS RENDRE. " + meta
    write_md(f"{args.sortie}.md", title, meta)
    write_pdf(f"{args.sortie}.pdf", title, meta)
    n_err = sum(1 for i in report if i[0] == "out" and i[1].startswith("ERREUR"))
    n_fail = sum(1 for i in report if i[0] == "out" and i[1].startswith("ÉCHEC"))
    print(f"\nTerminé : {args.sortie}.pdf et {args.sortie}.md "
          f"({n_err} requête(s) en erreur, {n_fail} assertion(s) en échec)")


if __name__ == "__main__":
    main()
