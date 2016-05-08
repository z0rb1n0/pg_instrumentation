BEGIN TRANSACTION;
SET TRANSACTION READ WRITE;

DROP VIEW IF EXISTS "instrumentation"."os_sys_cpu_threads";
DROP VIEW IF EXISTS "instrumentation"."os_sys_cpu_cores";
DROP VIEW IF EXISTS "instrumentation"."os_sys_cpu_pps";
DROP VIEW IF EXISTS "instrumentation"."os_sys_cpus";
DROP VIEW IF EXISTS "instrumentation"."sessions_status";
DROP VIEW IF EXISTS "instrumentation"."os_proc_ids_stat";
DROP VIEW IF EXISTS "instrumentation"."os_proc_io";
DROP VIEW IF EXISTS "instrumentation"."os_proc_processes_io";
DROP VIEW IF EXISTS "instrumentation"."os_proc_processes";
DROP VIEW IF EXISTS "instrumentation"."os_proc_loadavg";
DROP VIEW IF EXISTS "instrumentation"."os_proc_stat";

DROP FUNCTION IF EXISTS instrumentation.pg_read_file_soft(TEXT, BIGINT, BIGINT);
DROP FUNCTION IF EXISTS instrumentation.get_os_sys_cpus();
DROP FUNCTION IF EXISTS instrumentation.os_sys_cpu_pps();
DROP FUNCTION IF EXISTS instrumentation.os_sys_cpu_cores();
DROP FUNCTION IF EXISTS instrumentation.os_sys_cpu_threads();
DROP FUNCTION IF EXISTS instrumentation.get_os_proc_stat();
DROP FUNCTION IF EXISTS instrumentation.get_os_proc_loadavg();
DROP FUNCTION IF EXISTS instrumentation.get_os_proc_processes();
DROP FUNCTION IF EXISTS instrumentation.get_os_proc_processes_io();



GRANT USAGE ON SCHEMA "instrumentation" TO PUBLIC;

-- symlinks to /sys and /proc must be created in the postgresql directory for this to work
DO LANGUAGE 'plpgsql' $lc$
DECLARE required_symlink TEXT;
DECLARE failed_checks INT;
BEGIN

	failed_checks := 0;
	FOR required_symlink IN
		SELECT UNNEST(CAST('{sys,proc}' AS TEXT[]))
	LOOP
		IF (required_symlink NOT IN (SELECT pg_ls_dir('.'))) THEN
			failed_checks := (failed_checks + 1);
			RAISE WARNING 'Missing symlink in postgresql data dir: `%`. Session will be terminated', required_symlink;
		END IF;
	END LOOP;

	IF (failed_checks > 0) THEN
		SELECT pg_catalog.pg_terminate_backend(pg_catalog.pg_backend_pid());
	END IF;
END;
$lc$;

CREATE FUNCTION instrumentation.pg_read_file_soft(
	file_name TEXT,
	read_start BIGINT DEFAULT 0,
	read_length BIGINT DEFAULT 536870912
) RETURNS TEXT LANGUAGE 'plpgsql' VOLATILE RETURNS NULL ON NULL INPUT AS $CODE$
BEGIN
	RETURN pg_read_file(file_name, read_start, read_length);
EXCEPTION
	WHEN others THEN
		RETURN null;
END;
$CODE$;
COMMENT ON FUNCTION instrumentation.pg_read_file_soft(TEXT, BIGINT, BIGINT) IS 'Sometimes /proc files aren''t accessible due to race conditions. We turn it into a soft fail throug this wrapper';
GRANT EXECUTE ON FUNCTION "instrumentation"."pg_read_file_soft"(TEXT, BIGINT, BIGINT) TO PUBLIC;

CREATE FUNCTION instrumentation.get_os_sys_cpus() RETURNS TABLE (
	cpu_id SMALLINT,
	physical_package_id SMALLINT,
	core_siblings SMALLINT[],
	core_id SMALLINT,
	thread_siblings SMALLINT[],
	thread_id SMALLINT
) LANGUAGE 'sql' STABLE SECURITY DEFINER RETURNS NULL ON NULL INPUT COST 1000000000 AS $ctd$
	WITH
		advertised_cpu_dirs AS (
			SELECT
				CAST(REPLACE(scfs.scf_dir, 'cpu', '') AS SMALLINT) AS cpu_id,
				CONCAT('./sys/devices/system/cpu/', scfs.scf_dir, '/topology') AS cpu_topo_dir
			FROM
				pg_ls_dir('./sys/devices/system/cpu') AS scfs(scf_dir)
			WHERE
				(UPPER(scfs.scf_dir) ~ '^CPU[0-9]{1,14}$')
		)
		SELECT
			ad.cpu_id,
			CAST(pg_read_file(CONCAT(ad.cpu_topo_dir, '/physical_package_id'), 0, 4096) AS SMALLINT) AS physical_package_id,
			-- the following madness explodes lists of cpus specified as ranges and removes duplicates. unfortunately it has to be repeated twice (creating a function for this seems crazy)
			-- we assume that comma-separated fields come in two forms: single cpu id or range (in the hyphen-separated form)
			(
				SELECT
					array_agg(c.cpuid)
				FROM
					(
						SELECT DISTINCT ON ("cpuid")
							CAST((CASE
								WHEN (cfs.cfc ~ '^[0-9]{1,14}$') THEN
									CAST(cfs.cfc AS INT)
								ELSE
									generate_series(
										LEAST(CAST(split_part(cfs.cfc, '-', 1) AS INT), CAST(split_part(cfs.cfc, '-', 2) AS INT)),
										GREATEST(CAST(split_part(cfs.cfc, '-', 1) AS INT), CAST(split_part(cfs.cfc, '-', 2) AS INT)),
										1
									)
							END) AS SMALLINT) AS "cpuid"
						FROM
							UNNEST(CAST(CONCAT('{', pg_read_file(CONCAT(ad.cpu_topo_dir, '/core_siblings_list'), 0, 16384), '}') AS TEXT[])) AS cfs(cfc)
						ORDER BY
							cpuid ASC
					) AS c
			) AS core_siblings,
			CAST(pg_read_file(CONCAT(ad.cpu_topo_dir, '/core_id'), 0, 4096) AS SMALLINT) AS core_id,
			(
				SELECT
					array_agg(c.cpuid)
				FROM
					(
						SELECT DISTINCT ON ("cpuid")
							CAST((CASE
								WHEN (cfs.cfc ~ '^[0-9]{1,14}$') THEN
									CAST(cfs.cfc AS INT)
								ELSE
									generate_series(
										LEAST(CAST(split_part(cfs.cfc, '-', 1) AS INT), CAST(split_part(cfs.cfc, '-', 2) AS INT)),
										GREATEST(CAST(split_part(cfs.cfc, '-', 1) AS INT), CAST(split_part(cfs.cfc, '-', 2) AS INT)),
										1
									)
							END) AS SMALLINT) AS "cpuid"
						FROM
							UNNEST(CAST(CONCAT('{', pg_read_file(CONCAT(ad.cpu_topo_dir, '/thread_siblings_list'), 0, 16384), '}') AS TEXT[])) AS cfs(cfc)
						ORDER BY
							cpuid ASC
					) AS c
			) AS thread_siblings,
			CAST(null AS SMALLINT) AS thread_id
		FROM
			advertised_cpu_dirs AS ad
$ctd$;
COMMENT ON FUNCTION instrumentation.get_os_sys_cpus() IS '
Implementation of os_sys_cpus. Written as a function to bypass security restrictions.
List of CPUs, as presented in /sys/devices/system/cpu/cpu. The siblings lists are converted into to arrays of smallints. Some trickery (madness?) is used to ensure ordering and uniquess in the arrays
';
GRANT EXECUTE ON FUNCTION instrumentation.get_os_sys_cpus() TO PUBLIC;


CREATE VIEW instrumentation.os_sys_cpus AS
	SELECT * FROM instrumentation.get_os_sys_cpus();
;
COMMENT ON VIEW instrumentation.os_sys_cpus IS 'See invoked function';
GRANT SELECT ON TABLE instrumentation.os_sys_cpus TO PUBLIC;


CREATE VIEW instrumentation.os_sys_cpu_pps AS 
	SELECT DISTINCT ON(pp_id)
		CAST(UPPER(md5(array_to_string(ai.core_siblings, '_'))) AS CHAR(32)) AS pp_id,
		ai.core_siblings AS pp_cores
	FROM
		instrumentation.os_sys_cpus AS ai
;
COMMENT ON VIEW instrumentation.os_sys_cpu_pps IS 'List of processor physical packages as presented in /sys/devices/system/cpu/cpu. Advertised id is disregarded and a new one is inferred by grouping by list of cores (whose MD5 hash serves as package "id". there is a chance for collisions but it wouldn''t be a big deal';
GRANT SELECT ON TABLE instrumentation.os_sys_cpu_pps TO PUBLIC;

CREATE VIEW instrumentation.os_sys_cpu_cores AS 
	SELECT DISTINCT ON(core_id)
		CAST(UPPER(md5(array_to_string(ai.thread_siblings, '_'))) AS CHAR(32)) AS core_id,
		CAST(UPPER(md5(array_to_string(ai.core_siblings, '_'))) AS CHAR(32)) AS pp_id,
		ai.thread_siblings AS core_threads
	FROM
		instrumentation.os_sys_cpus AS ai
;
COMMENT ON VIEW instrumentation.os_sys_cpu_cores IS 'List of processor cores as presented in /sys/devices/system/cpu/cpu. Advertised id is disregarded and a new one is inferred by grouping by list of threads. See instrumentation.os_sys_cpu_pps for details';
GRANT SELECT ON TABLE instrumentation.os_sys_cpu_cores TO PUBLIC;

CREATE VIEW instrumentation.os_sys_cpu_threads AS 
	SELECT
		ai.cpu_id AS thread_id,
		CAST(UPPER(md5(array_to_string(ai.thread_siblings, '_'))) AS CHAR(32)) AS core_id
	FROM
		instrumentation.os_sys_cpus AS ai
;
COMMENT ON VIEW instrumentation.os_sys_cpu_threads IS 'List of processor threads as presented in /sys/devices/system/cpu/cpu. No grouping involved but the core ID is inferred by the list of siblings';
GRANT SELECT ON TABLE instrumentation.os_sys_cpus TO PUBLIC;




CREATE FUNCTION instrumentation.get_os_proc_stat() RETURNS TABLE (
	cpu_id SMALLINT,
	jiffies_user BIGINT,
	jiffies_nice BIGINT,
	jiffies_system BIGINT,
	jiffies_idle BIGINT,
	jiffies_iowait BIGINT,
	jiffies_irq BIGINT,
	jiffies_softirq BIGINT,
	jiffies_steal BIGINT,
	jiffies_guest BIGINT
) LANGUAGE 'sql' STABLE SECURITY DEFINER RETURNS NULL ON NULL INPUT COST 1000000000 AS $ops$
	SELECT
		CAST((CASE WHEN (cpu_stat.cpu_fields[1] ~ '^[0-9]+$') THEN cpu_stat.cpu_fields[1] ELSE null END) AS SMALLINT) AS cpu_id,
		CAST(cpu_stat.cpu_fields[2] AS BIGINT) AS jiffies_user,
		CAST(cpu_stat.cpu_fields[3] AS BIGINT) AS jiffies_nice,
		CAST(cpu_stat.cpu_fields[4] AS BIGINT) AS jiffies_system,
		CAST(cpu_stat.cpu_fields[5] AS BIGINT) AS jiffies_idle,
		CAST(cpu_stat.cpu_fields[6] AS BIGINT) AS jiffies_iowait,
		CAST(cpu_stat.cpu_fields[7] AS BIGINT) AS jiffies_irq,
		CAST(cpu_stat.cpu_fields[8] AS BIGINT) AS jiffies_softirq,
		CAST(cpu_stat.cpu_fields[9] AS BIGINT) AS jiffies_steal,
		CAST(cpu_stat.cpu_fields[10] AS BIGINT) AS jiffies_guest
	FROM
		(
			SELECT
				regexp_split_to_array(regexp_replace(UPPER(st.row_text), '^CPU', ''), '\s+') AS cpu_fields
			FROM
				regexp_split_to_table(pg_read_file('./proc/stat', 0, 16777216), '\n', 'n') AS st(row_text)
			WHERE
				UPPER(st.row_text) ~ '^CPU[0-9]*\s'
		) AS cpu_stat
$ops$;
COMMENT ON FUNCTION instrumentation.get_os_proc_stat() IS '
Implementation of os_proc_stat. Written as a function to bypass security restrictions.
A relalational friendly rendering of /proc/stat, FOR CPU ACTIVITY INFO ONLY!!!
';
GRANT EXECUTE ON FUNCTION instrumentation.get_os_proc_stat() TO PUBLIC;



CREATE VIEW instrumentation.os_proc_stat AS
	SELECT * FROM instrumentation.get_os_proc_stat();
;
COMMENT ON VIEW instrumentation.os_proc_stat IS 'See invoked function';
GRANT SELECT ON TABLE instrumentation.os_proc_stat TO PUBLIC;


CREATE FUNCTION instrumentation.get_os_proc_loadavg() RETURNS TABLE (
	loadavg_60s FLOAT,
	loadavg_300s FLOAT,
	loadavg_900s FLOAT,
	tasks_runnable BIGINT,
	tasks_total BIGINT,
	last_pid BIGINT
) LANGUAGE 'sql' STABLE SECURITY DEFINER RETURNS NULL ON NULL INPUT COST 1000000000 AS $opl$
	WITH
		loadavg AS (
			SELECT
				regexp_split_to_array(regexp_replace(pg_read_file('./proc/loadavg', 0, 65536), '\n|\r', ''), '\s+') AS f
		)
	SELECT
		CAST(l.f[1] AS DOUBLE PRECISION) AS loadavg_60s,
		CAST(l.f[2] AS DOUBLE PRECISION) AS loadavg_300s,
		CAST(l.f[3] AS DOUBLE PRECISION) AS loadavg_900s,
		CAST(split_part(l.f[4], '/', 1) AS BIGINT) AS tasks_runnable,
		CAST(split_part(l.f[4], '/', 2) AS BIGINT) AS tasks_total,
		CAST(l.f[5] AS BIGINT) AS last_pid
	FROM
		loadavg AS l

$opl$;
COMMENT ON FUNCTION instrumentation.get_os_proc_loadavg() IS '
Implementation of os_proc_loadavg. Written as a function to bypass security restrictions.
A relalational friendly rendering of /proc/loadavg
';
GRANT EXECUTE ON FUNCTION instrumentation.get_os_proc_loadavg() TO PUBLIC;

CREATE VIEW "instrumentation"."os_proc_loadavg" AS
	SELECT * FROM instrumentation.get_os_proc_loadavg();
;
COMMENT ON VIEW "instrumentation"."os_proc_loadavg" IS 'See invoked function';
GRANT SELECT ON TABLE "instrumentation"."os_proc_loadavg" TO PUBLIC;



CREATE FUNCTION instrumentation.get_os_proc_processes() RETURNS TABLE (
	pid BIGINT,
	comm VARCHAR(256),
	"state" CHAR(1),
	ppid BIGINT,
	pgrp BIGINT,
	"session" BIGINT,
	tty_nr BIGINT,
	tpgid BIGINT,
	flags BIGINT,
	minflt NUMERIC(20, 0),
	cminflt NUMERIC(20, 0),
	majflt NUMERIC(20, 0),
	cmajflt NUMERIC(20, 0),
	utime NUMERIC(20, 0),
	stime NUMERIC(20, 0),
	cutime NUMERIC(20, 0),
	cstime NUMERIC(20, 0),
	priority NUMERIC(20, 0),
	nice NUMERIC(20, 0),
	num_threads NUMERIC(20, 0),
	itrealvalue NUMERIC(20, 0),
	starttime NUMERIC(20, 0),
	vsize NUMERIC(20, 0),
	rss NUMERIC(20, 0),
	rsslim NUMERIC(20, 0),
	startcode NUMERIC(20, 0),
	endcode NUMERIC(20, 0),
	startstack NUMERIC(20, 0),
	kstkesp NUMERIC(20, 0),
	kstkeip NUMERIC(20, 0),
	signal NUMERIC(20, 0),
	blocked NUMERIC(20, 0),
	sigignore NUMERIC(20, 0),
	sigcatch NUMERIC(20, 0),
	wchan NUMERIC(20, 0),
	nswap NUMERIC(20, 0),
	cnswap NUMERIC(20, 0),
	exit_signal NUMERIC(20, 0),
	processor NUMERIC(20, 0),
	rt_priority NUMERIC(20, 0),
	policy NUMERIC(20, 0),
	delayacct_blkio_ticks NUMERIC(20, 0),
	guest_time NUMERIC(20, 0),
	cguest_time NUMERIC(20, 0)
) LANGUAGE 'sql' STABLE SECURITY DEFINER RETURNS NULL ON NULL INPUT COST 1000000000 AS $opp$
	SELECT
		CAST(p.proc_stat_data[1] AS BIGINT) AS pid,
		CAST(p.proc_stat_data[2] AS VARCHAR(256)) AS comm,
		CAST(p.proc_stat_data[3] AS CHAR(1)) AS "state",
		CAST(p.proc_stat_data[4] AS BIGINT) AS ppid,
		CAST(p.proc_stat_data[5] AS BIGINT) AS pgrp,
		CAST(p.proc_stat_data[6] AS BIGINT) AS "session",
		CAST(p.proc_stat_data[7] AS BIGINT) AS tty_nr,
		CAST(p.proc_stat_data[8] AS BIGINT) AS tpgid,
		CAST(p.proc_stat_data[9] AS BIGINT) AS flags,
		CAST(p.proc_stat_data[10] AS NUMERIC(20, 0)) AS minflt,
		CAST(p.proc_stat_data[11] AS NUMERIC(20, 0)) AS cminflt,
		CAST(p.proc_stat_data[12] AS NUMERIC(20, 0)) AS majflt,
		CAST(p.proc_stat_data[13] AS NUMERIC(20, 0)) AS cmajflt,
		CAST(p.proc_stat_data[14] AS NUMERIC(20, 0)) AS utime,
		CAST(p.proc_stat_data[15] AS NUMERIC(20, 0)) AS stime,
		CAST(p.proc_stat_data[16] AS NUMERIC(20, 0)) AS cutime,
		CAST(p.proc_stat_data[17] AS NUMERIC(20, 0)) AS cstime,
		CAST(p.proc_stat_data[18] AS NUMERIC(20, 0)) AS priority,
		CAST(p.proc_stat_data[19] AS NUMERIC(20, 0)) AS nice,
		CAST(p.proc_stat_data[20] AS NUMERIC(20, 0)) AS num_threads,
		CAST(p.proc_stat_data[21] AS NUMERIC(20, 0)) AS itrealvalue,
		CAST(p.proc_stat_data[22] AS NUMERIC(20, 0)) AS starttime,
		CAST(p.proc_stat_data[23] AS NUMERIC(20, 0)) AS vsize,
		CAST(p.proc_stat_data[24] AS NUMERIC(20, 0)) AS rss,
		CAST(p.proc_stat_data[25] AS NUMERIC(20, 0)) AS rsslim,
		CAST(p.proc_stat_data[26] AS NUMERIC(20, 0)) AS startcode,
		CAST(p.proc_stat_data[27] AS NUMERIC(20, 0)) AS endcode,
		CAST(p.proc_stat_data[28] AS NUMERIC(20, 0)) AS startstack,
		CAST(p.proc_stat_data[29] AS NUMERIC(20, 0)) AS kstkesp,
		CAST(p.proc_stat_data[30] AS NUMERIC(20, 0)) AS kstkeip,
		CAST(p.proc_stat_data[31] AS NUMERIC(20, 0)) AS signal,
		CAST(p.proc_stat_data[32] AS NUMERIC(20, 0)) AS blocked,
		CAST(p.proc_stat_data[33] AS NUMERIC(20, 0)) AS sigignore,
		CAST(p.proc_stat_data[34] AS NUMERIC(20, 0)) AS sigcatch,
		CAST(p.proc_stat_data[35] AS NUMERIC(20, 0)) AS wchan,
		CAST(p.proc_stat_data[36] AS NUMERIC(20, 0)) AS nswap,
		CAST(p.proc_stat_data[37] AS NUMERIC(20, 0)) AS cnswap,
		CAST(p.proc_stat_data[38] AS NUMERIC(20, 0)) AS exit_signal,
		CAST(p.proc_stat_data[39] AS NUMERIC(20, 0)) AS processor,
		CAST(p.proc_stat_data[40] AS NUMERIC(20, 0)) AS rt_priority,
		CAST(p.proc_stat_data[41] AS NUMERIC(20, 0)) AS policy,
		CAST(p.proc_stat_data[42] AS NUMERIC(20, 0)) AS delayacct_blkio_ticks,
		CAST(p.proc_stat_data[43] AS NUMERIC(20, 0)) AS guest_time,
		CAST(p.proc_stat_data[44] AS NUMERIC(20, 0)) AS cguest_time
	FROM
		(
			SELECT
				-- some retarded process injects white spaces into the process image name (I'm looking at you, EMC HBAs driver).
				-- This makes separating /proc/<pid>/stat a fields a little tricky. We rely on brackets first
				(
					ARRAY[
						stat_files.image_split[1], -- PID
						stat_files.image_split[2] -- argv0
					]
				||
					STRING_TO_ARRAY(stat_files.image_split[3], ' ') -- everything else
				) AS proc_stat_data
				
			FROM
				(
					SELECT
						regexp_split_to_array(REPLACE(instrumentation.pg_read_file_soft('./proc/' || dir_list.item_name || '/stat', 0, 65536), CHR(10), ''), '\s+\(|\)\s+') AS image_split
					FROM
						(SELECT pd.proc_dir FROM pg_ls_dir('./proc') AS pd(proc_dir) WHERE pd.proc_dir ~ '^[0-9]{1,14}$') AS dir_list(item_name)
				) AS stat_files
		) AS p
;$opp$;
COMMENT ON FUNCTION instrumentation.get_os_proc_processes() IS '
Implementation of os_proc_processes. Written as a function to bypass security restrictions.
basically the contents of /proc/<pid>/stat, in a relational-friendly way.
Unfortunately many of the exported values are numeric representation of 64 bit unsigned longs,
which force us to use numeric (the alternative would be altering the actual value by casting them to signed)
';
GRANT EXECUTE ON FUNCTION instrumentation.get_os_proc_processes() TO PUBLIC;


CREATE VIEW instrumentation.os_proc_processes AS
	SELECT * FROM instrumentation.get_os_proc_processes()
;
COMMENT ON VIEW instrumentation.os_proc_processes IS 'See invoked function';
GRANT SELECT ON TABLE instrumentation.os_proc_processes TO PUBLIC;





CREATE FUNCTION instrumentation.get_os_proc_processes_io() RETURNS TABLE (
	pid BIGINT,
	rchar NUMERIC(20, 0),
	wchar NUMERIC(20, 0),
	syscr NUMERIC(20, 0),
	syscw NUMERIC(20, 0),
	read_bytes NUMERIC(20, 0),
	write_bytes NUMERIC(20, 0),
	cancelled_write_bytes NUMERIC(20, 0)
) LANGUAGE 'sql' STABLE SECURITY DEFINER RETURNS NULL ON NULL INPUT COST 1000000000 AS $oppio$
	SELECT
		CAST(osp.proc_pid AS BIGINT) AS pid,
		CAST(regexp_replace(regexp_replace(osp.proc_stat_data, '.*(\n|^)rchar: *', '', ''), '[^0-9].*', '') AS NUMERIC(20, 0)) AS rchar,
		CAST(regexp_replace(regexp_replace(osp.proc_stat_data, '.*(\n|^)wchar: *', '', ''), '[^0-9].*', '') AS NUMERIC(20, 0)) AS wchar,
		CAST(regexp_replace(regexp_replace(osp.proc_stat_data, '.*(\n|^)syscr: *', '', ''), '[^0-9].*', '') AS NUMERIC(20, 0)) AS syscr,
		CAST(regexp_replace(regexp_replace(osp.proc_stat_data, '.*(\n|^)syscw: *', '', ''), '[^0-9].*', '') AS NUMERIC(20, 0)) AS syscw,
		CAST(regexp_replace(regexp_replace(osp.proc_stat_data, '.*(\n|^)read_bytes: *', '', ''), '[^0-9].*', '') AS NUMERIC(20, 0)) AS read_bytes,
		CAST(regexp_replace(regexp_replace(osp.proc_stat_data, '.*(\n|^)write_bytes: *', '', ''), '[^0-9].*', '') AS NUMERIC(20, 0)) AS write_bytes,
		CAST(regexp_replace(regexp_replace(osp.proc_stat_data, '.*(\n|^)cancelled_write_bytes: *', '', ''), '[^0-9].*', '') AS NUMERIC(20, 0)) AS cancelled_write_bytes
	FROM
		(
			SELECT
				CAST(d.item_name AS BIGINT) AS proc_pid,
				instrumentation.pg_read_file_soft('./proc/' || d.item_name || '/io', 0, 65536) AS proc_stat_data
			FROM
				(SELECT pg_ls_dir('./proc')) AS d(item_name)
			WHERE
				(d.item_name ~ '^[0-9]+$')
		) AS osp
$oppio$;
COMMENT ON FUNCTION instrumentation.get_os_proc_processes_io() IS '
Implementation of os_proc_processes_io. Written as a function to bypass security restrictions.
basically the contents of /proc/<pid>/io, in a relational-friendly way. It returns usable info about postgresql backend processes only (due to /proc/<pid>/io permissions).
It uses numeric to prevent signed int overflow from unsigned long input :(
';
GRANT EXECUTE ON FUNCTION instrumentation.get_os_proc_processes_io() TO PUBLIC;


CREATE VIEW instrumentation.os_proc_processes_io AS
	SELECT * FROM instrumentation.get_os_proc_processes_io()
;
COMMENT ON VIEW instrumentation.os_proc_processes_io IS 'See invoked function';
GRANT SELECT ON TABLE instrumentation.os_proc_processes_io TO PUBLIC;


CREATE VIEW instrumentation.sessions_status AS
	SELECT
		p.pid AS session_pid,
		ps."state" AS process_state,
		ps.utime AS process_utime,
		ps.stime AS process_stime,
		pio.rchar AS process_rchar,
		pio.wchar AS process_wchar,
		usename AS user_name,
		datname AS database_name,
		application_name AS app_name,
		CAST((EXTRACT('epoch' FROM (clock_timestamp() - p.backend_start)) * 1000.0) AS BIGINT) AS process_age,
		CAST((CASE WHEN (UPPER(p.state) ~ '((ACTIVE)|(FASTPATH)|(IN TRANSACTION))') THEN (EXTRACT('epoch' FROM (clock_timestamp() - p.query_start)) * 1000.0) ELSE -1.0 END) AS INT) AS statement_age,
		(CASE WHEN p.waiting THEN gl.pid ELSE null END) AS blocking_lock_pid,
		(
			SELECT
				lr.relname
			FROM
				pg_locks AS lrl INNER JOIN pg_class AS lr
				ON
					(lr.oid = lrl.relation)
			WHERE
				(lrl.virtualtransaction = wl.virtualtransaction)
			AND
				(NOT (lrl.relation IS NULL))
			ORDER BY
				(UPPER(lrl.locktype) = 'TUPLE') DESC
			LIMIT 1
		) AS lock_relation,
		p.query AS query_sql
	FROM
		(
			(
				(
					pg_stat_activity AS p INNER JOIN instrumentation.os_proc_processes AS ps
					ON
						(ps.pid = p.pid)
				) INNER JOIN instrumentation.os_proc_processes_io AS pio
				ON
					(pio.pid = p.pid)
			) LEFT JOIN pg_locks AS wl
			ON
				(p.datname = current_database())
			AND
				(wl.pid = p.pid)
			AND
				(NOT wl.granted)
		) LEFT JOIN pg_locks AS gl
		ON
			(gl.transactionid = wl.transactionid)
		AND
			gl.granted
;
COMMENT ON VIEW instrumentation.sessions_status IS 'pg_stat_activity on steroids. It shows the locking process/relation and retrieves additional information from the OS (from /proc)';

COMMIT TRANSACTION;