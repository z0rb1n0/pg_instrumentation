BEGIN TRANSACTION;
SET TRANSACTION READ WRITE;


CREATE OR REPLACE PROCEDURAL LANGUAGE 'plperlu';

CREATE SCHEMA IF NOT EXISTS "instrumentation";

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

DROP FUNCTION IF EXISTS instrumentation.read_file_abs(TEXT, BIGINT, BIGINT);
DROP FUNCTION IF EXISTS instrumentation.get_os_sys_cpus();
DROP FUNCTION IF EXISTS instrumentation.os_sys_cpu_pps();
DROP FUNCTION IF EXISTS instrumentation.os_sys_cpu_cores();
DROP FUNCTION IF EXISTS instrumentation.os_sys_cpu_threads();
DROP FUNCTION IF EXISTS instrumentation.get_os_proc_stat();
DROP FUNCTION IF EXISTS instrumentation.get_os_proc_loadavg();
DROP FUNCTION IF EXISTS instrumentation.get_os_proc_processes();
DROP FUNCTION IF EXISTS instrumentation.get_os_proc_processes_io();



GRANT USAGE ON SCHEMA "instrumentation" TO PUBLIC;

CREATE FUNCTION instrumentation.ls_dir_abs(
	dir_name TEXT
) RETURNS SETOF TEXT LANGUAGE 'plperlu' SECURITY INVOKER STABLE RETURNS NULL ON NULL INPUT AS $CODE$
use strict;
use warnings;

my $d_h;

	opendir($d_h, $_[0]) || return(undef);
	while (my $next_dir = readdir($d_h)) {
		# the builtin skips . and ..
		(($next_dir ne ".") && ($next_dir ne "..")) && return_next($next_dir);
	}
	closedir($d_h) || exit(undef);
	return undef;
$CODE$;
REVOKE ALL PRIVILEGES ON FUNCTION instrumentation.ls_dir_abs(text) FROM PUBLIC; -- just for good measure
COMMENT ON FUNCTION instrumentation.ls_dir_abs(text) IS 'Insecure version of pg_ls_dir. This allows the listing of any directory in the file system the backend process has access to';


CREATE FUNCTION instrumentation.read_binary_files_abs(
	file_names TEXT[],
	read_start BIGINT DEFAULT 0,
	read_length BIGINT DEFAULT 536870912
) RETURNS TABLE (
	fn TEXT,
	fd BYTEA
) LANGUAGE 'plperlu' SECURITY INVOKER STABLE RETURNS NULL ON NULL INPUT AS $CODE$
use strict;
use warnings;
use Fcntl;

	foreach my $next_file (@{$_[0]}) {
		my $f_h;
		my $next_buf = undef;
		if (sysopen($f_h, $next_file, Fcntl::O_RDONLY)) {

			defined($_[1]) && sysseek($f_h, $_[1], Fcntl::SEEK_SET);

			sysread($f_h, $next_buf, $_[2]) || ($next_buf = undef);
			
			close($f_h) || exit(undef);
		}
		return_next({"fn" => $next_file, "fd" => $next_buf});
	}
	return undef;

$CODE$;
REVOKE ALL PRIVILEGES ON FUNCTION instrumentation.read_binary_files_abs(TEXT[], BIGINT, BIGINT) FROM PUBLIC; -- just for good measure
COMMENT ON FUNCTION instrumentation.read_binary_files_abs(TEXT[], BIGINT, BIGINT) IS 'Multi-file version of read_binary_file_abs (see below). Allows the passing of multiple file names as an array.
This exists for optimization reasons: plperlu starts a new interpreter for each function call, which makes read_binary_file_abs inefficient for a large number of files.
Returns a row for each input file. Output columns:
	fn (file name): the full path of the file
	fd (file data): data retrieved for the specified file. Null
';

CREATE FUNCTION instrumentation.read_binary_file_abs(
	file_name TEXT,
	read_start BIGINT DEFAULT 0,
	read_length BIGINT DEFAULT 536870912
) RETURNS BYTEA LANGUAGE 'sql' SECURITY INVOKER STABLE RETURNS NULL ON NULL INPUT AS $CODE$
	SELECT
		rbfa.fd
	FROM
		instrumentation.read_binary_files_abs(ARRAY[$1], $2, $3) AS rbfa
	;
$CODE$;
REVOKE ALL PRIVILEGES ON FUNCTION instrumentation.read_binary_file_abs(TEXT, BIGINT, BIGINT) FROM PUBLIC; -- just for good measure
COMMENT ON FUNCTION instrumentation.read_binary_file_abs(TEXT, BIGINT, BIGINT) IS 'Insecure version of pg_read_binary_file. This allows reading of any file the backend process has access to.
Actually implemented as a single-file wrapper for read_binary_files_abs()';


CREATE FUNCTION instrumentation.read_files_abs(
	file_names TEXT[],
	read_start BIGINT DEFAULT 0,
	read_length BIGINT DEFAULT 536870912
) RETURNS TABLE (
	fn TEXT,
	fd TEXT
) LANGUAGE 'sql' SECURITY INVOKER STABLE RETURNS NULL ON NULL INPUT AS $CODE$
	SELECT
		fn,
		convert_from(rbfa.fd, (SELECT s.setting FROM pg_settings AS s WHERE (s.name = 'client_encoding')))
	FROM
		instrumentation.read_binary_files_abs($1, $2, $3) AS rbfa
	;
$CODE$;
REVOKE ALL PRIVILEGES ON FUNCTION instrumentation.read_files_abs(TEXT[], BIGINT, BIGINT) FROM PUBLIC; -- just for good measure
COMMENT ON FUNCTION instrumentation.read_binary_files_abs(TEXT[], BIGINT, BIGINT) IS 'Multi-file version of read_file_abs (see below). Allows the passing of multiple file names as an array.
Same result set as read_binary_files_abs, with the exception that fd is an encoding-safe text column (in fact, it is just a wrapper for it)';


CREATE FUNCTION instrumentation.read_file_abs(
	file_name TEXT,
	read_start BIGINT DEFAULT 0,
	read_length BIGINT DEFAULT 67108864
) RETURNS TEXT LANGUAGE 'sql' SECURITY INVOKER STABLE RETURNS NULL ON NULL INPUT AS $CODE$
	SELECT
		rbfa.fd
	FROM
		instrumentation.read_files_abs(ARRAY[$1], $2, $3) AS rbfa
	;
$CODE$;
REVOKE ALL PRIVILEGES ON FUNCTION instrumentation.read_file_abs(TEXT, BIGINT, BIGINT) FROM PUBLIC; -- just for good measure
COMMENT ON FUNCTION instrumentation.read_file_abs(TEXT, BIGINT, BIGINT) IS 'Insecure version of pg_read_file. This allows reading of any file the backend process has access to.
Actually implemented as a single-file wrapper for read_files_abs()';






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
				CONCAT('/sys/devices/system/cpu/', scfs.scf_dir, '/topology') AS cpu_topo_dir
			FROM
				instrumentation.ls_dir_abs('/sys/devices/system/cpu') AS scfs(scf_dir)
			WHERE
				(UPPER(scfs.scf_dir) ~ '^CPU[0-9]{1,14}$')
		)
		SELECT
			ad.cpu_id,
			instrumentation.read_file_abs(CONCAT(ad.cpu_topo_dir, '/physical_package_id'), 0, 4096)::SMALLINT AS physical_package_id,
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
							UNNEST(CAST(CONCAT('{', instrumentation.read_file_abs(CONCAT(ad.cpu_topo_dir, '/core_siblings_list'), 0, 16384), '}') AS TEXT[])) AS cfs(cfc)
						ORDER BY
							cpuid ASC
					) AS c
			) AS core_siblings,
			CAST(instrumentation.read_file_abs(CONCAT(ad.cpu_topo_dir, '/core_id'), 0, 4096) AS SMALLINT) AS core_id,
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
							UNNEST(CAST(CONCAT('{', instrumentation.read_file_abs(CONCAT(ad.cpu_topo_dir, '/thread_siblings_list'), 0, 16384), '}') AS TEXT[])) AS cfs(cfc)
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
List of CPUs, as presented in /sys/devices/system/cpu/cpu. The siblings lists are converted into to arrays of smallints. Some trickery (madness?) is used to ensure ordering and uniquess in the arrays.

This function has not been optimized for the new multi-file access functionality yet
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
	-- it is very important that we use CTEs here as the perl functions need to be materialized
	WITH
		cpu_stat AS (
				SELECT
					regexp_split_to_array(regexp_replace(UPPER(st.row_text), '^CPU', ''), '\s+') AS cpu_fields
				FROM
					regexp_split_to_table(instrumentation.read_file_abs('/proc/stat', 0, 1048576), '\n', 'n') AS st(row_text)
				WHERE
					UPPER(st.row_text) ~ '^CPU[0-9]*\s'
		)
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
		cpu_stat
$ops$;
COMMENT ON FUNCTION instrumentation.get_os_proc_stat() IS '
Implementation of os_proc_stat. Written as a function to bypass security restrictions.
A relalational friendly rendering of /proc/stat, FOR CPU ACTIVITY INFO ONLY!!!
Note that an NULL cpu_id represents the grand total
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
				regexp_split_to_array(regexp_replace(instrumentation.read_file_abs('/proc/loadavg', 0, 65536), '\n|\r', ''), '\s+') AS f
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
	-- it is very important that we use CTEs here as the perl functions need to be materialized
	WITH
		p AS (
			SELECT
				-- some retarded process injects white spaces into the process image name (I'm looking at you, EMC HBAs driver).
				-- This makes separating /proc/<pid>/stat a fields a little tricky. We rely on brackets first
				(
					ARRAY[
						stat_files.image_split[1], -- PID
						stat_files.image_split[2]  -- argv0
					]
				||
					STRING_TO_ARRAY(stat_files.image_split[3], ' ') -- everything else
				) AS proc_stat_data
				
			FROM
				-- we use our multi-file open function
				(
					SELECT
						regexp_split_to_array(REPLACE(rf.fd, CHR(10), ''), '\s+\(|\)\s+') AS image_split
					FROM
						instrumentation.read_files_abs(
							(SELECT array_agg(CONCAT('/proc/', pd.proc_dir, '/stat')) FROM instrumentation.ls_dir_abs('/proc') AS pd(proc_dir) WHERE (pd.proc_dir ~ '^[0-9]{1,14}$')),
							0,
							65536
						) AS rf
				) AS stat_files
		)
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
		p
	;
$opp$;
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
	-- it is very important that we use CTEs here as the perl functions need to be materialized
	WITH
		osp AS (
			SELECT
				split_part(rf.fn, '/', 3)::BIGINT AS proc_pid,
				rf.fd AS proc_stat_data
			FROM
				instrumentation.read_files_abs(
					(SELECT array_agg(CONCAT('/proc/', pd.item_name, '/io')) FROM instrumentation.ls_dir_abs('/proc') AS pd(item_name) WHERE (pd.item_name ~ '^[0-9]+$')),
					0,
					65536
				) AS rf
		)
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
		osp
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
