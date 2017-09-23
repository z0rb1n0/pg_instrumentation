#! /usr/bin/php -qC
<?php


// highlight_mode is an hybrid code/bitmask.
// first 3 bits are the following color codes:
define("TERM_COLOR_BLACK", 0);
define("TERM_COLOR_RED", 1);
define("TERM_COLOR_GREEN", 2);
define("TERM_COLOR_YELLOW", 3);
define("TERM_COLOR_BLUE", 4);
define("TERM_COLOR_MAGENTA", 5);
define("TERM_COLOR_CYAN", 6);
define("TERM_COLOR_WHITE", 7);

// following bits are actual flags:
define("TERM_MODE_BOLD", pow(2, 3));
define("TERM_MODE_DIM", pow(2, 4));
define("TERM_MODE_UNDERLINE", pow(2, 5));
define("TERM_MODE_REVERSE", pow(2, 6));
define("TERM_MODE_STANDOUT", pow(2, 7));


define("DEFAULT_DB_HOST", "/tmp");
define("DEFAULT_DB_PORT", 5432);
define("DEFAULT_DB_USER", "postgres");
define("DEFAULT_DB_PASSWORD", "postgres");
define("DEFAULT_DB_NAME", "postgres");




define("CONNECTION_RETRY_INTERVAL", 5000); // in milliseconds
define("POLL_INTERVAL", 2000); // in milliseconds
define("MAX_SQL_OUTPUT_LINES", 1);
define("PROCESS_LOG_MAX_CONNECTIONS_PERCENTAGE", 80.0);
define("STATEMENT_AGE_ALERT_THRESHOLD", 1000);
define("GET_SLAVE_PROCESSES", 1);


// this is a wild assumption...Can't remove it without more server-side code.
// Unfortunately we're left with 2 options there: plperlu, or finding a /proc
// entry that exposes that setting (I couldn't)
define("HOST_SC_CLK_TCK", 100);


define("LOG_FILE", "/dev/null");


ini_set("error_reporting", E_ALL);

// dummy error handler to make everything fatal
function just_croak($errno, $errstr, $errfile, $errline, $errcontext) {

	printf("PHP Fatal error:  %1\$s in %2\$s on line %3\$s\n", $errstr, $errfile, $errline);
	
	die();
	
}

function usage($error) {
    if($error)
        printf("Invalid parameters.\n");
    printf("Usage:\n");
    printf("\t./pg_top.php [-U <username>] [-h <hostname>] [-p <port>] [-W <password>] [<database_name>]\n");
    die();
}

function print_connection_info($input_param_map) {
    printf("-------------------------------------------------------------------------------\n");
    printf("Connecting %1\$s:%2\$d using user %3\$s and database %4\$s\n",
		$input_param_map["DB_HOST"],
		$input_param_map["DB_PORT"],
		$input_param_map["DB_USER"],
		$input_param_map["DB_NAME"]
	);
    printf("-------------------------------------------------------------------------------\n");
}

function build_param_map($args) {
    $i = 1;
    $map = [];
    while($i < sizeof($args)) {
        switch ($args[$i]) {
            case "--help" :
                usage(false);
            case "-U" :
                $map["DB_USER"] = $args[++$i];
                break;
            case "-h" :
                $ip = $args[++$i];
                $map["DB_HOST"] = $ip;
                break;
            case "-p" :
                $map["DB_PORT"] = $args[++$i];
                break;
            case "-W" :
                $map["DB_PASSWORD"] = $args[++$i];
                break;
            default:
                if(array_key_exists("DB_NAME", $map))
                    usage(true);
                else
                    $map["DB_NAME"] = $args[$i];
        }
        $i++;
    }
    return $map;
}

function get_log_line_prefix() {
	$current_microtime = microtime(true);
	return sprintf("%1\$s.%2\$03d: ", strftime("%Y-%m-%d %H:%M:%S", ((int) floor($current_microtime))), ((fmod($current_microtime, 1.0)) * 1000));
}

// the %1\$s should be replaced by the following when data is available
/*
 *	UNION ALL
 *		VALUES (...)
 */
 
$max_connections_sql = "SELECT SUM(CAST(setting AS INT) * (CASE UPPER(name) WHEN 'SUPERUSER_RESERVED_CONNECTIONS' THEN -1 ELSE 1 END)) as max_connections FROM pg_settings WHERE UPPER(name) IN ('MAX_CONNECTIONS', 'SUPERUSER_RESERVED_CONNECTIONS');";
 
 
$processes_sql = "
SELECT
	session_pid,
	process_state,
	process_utime,
	process_stime,
	process_rchar,
	process_wchar,
	user_name,
	database_name,
	app_name,
	process_age,
	statement_age,
	blocking_lock_pid,
	lock_relation,
	query_sql,
	EXTRACT('epoch' FROM clock_timestamp())::FLOAT AS epoch
FROM
	instrumentation.sessions_status
;
";



	// enable for debugging
	set_error_handler("just_croak");



	$param_map = build_param_map($argv);

	# we pick the values from whichever source they come first
	$consts = get_defined_constants();

	foreach (["DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME"] as $c_param) {
		$param_map[$c_param] = array_key_exists($c_param, $param_map) ? $param_map[$c_param] : $consts["DEFAULT_" . $c_param];
	}
	unset($consts);
	print_connection_info($param_map);

	// If the host is not a local socket, lets resolve FQDN first to avoid layout problems
	if (substr($param_map["DB_HOST"], 0, 1) != "/") {
		$param_map["DB_HOST"] = gethostbyname($param_map["DB_HOST"]);
	}


	getenv("TERM") || trigger_error("Not running on a terminal. Goodbye", E_USER_ERROR);

	$screen_width = ((int) exec("tput cols"));
	$screen_height = ((int) exec("tput lines"));
	$clear_scr_seq  = exec("tput clear");
	$anomaly_header_seq = exec("tput setaf 1; tput smso");

	$term_colors = array();
	foreach (array(
		TERM_COLOR_BLACK,
		TERM_COLOR_RED,
		TERM_COLOR_GREEN,
		TERM_COLOR_YELLOW,
		TERM_COLOR_BLUE,
		TERM_COLOR_MAGENTA,
		TERM_COLOR_CYAN,
		TERM_COLOR_WHITE,
	) as $term_col_code) {
		$term_colors[$term_col_code] = exec("tput setaf " . $term_col_code);
	}


	$term_go_bold = exec("tput bold");

	$term_reset_mode_seq = exec("tput sgr0");


	while (true) {


		// we construct a structured list of all hosts. The master is just part of it
		// the list of hosts is built on every loop based on wheter or not slaves are used/desired

		$db_cnn_string = sprintf("host='%1\$s' port=%2\$d user='%3\$s' password='%4\$s' dbname='%5\$s' application_name='%6\$s'",
			$param_map["DB_HOST"],
			$param_map["DB_PORT"],
			$param_map["DB_USER"],
			$param_map["DB_PASSWORD"],
			$param_map["DB_NAME"],
			((isset($_SERVER['REQUEST_URI']) && strlen($_SERVER['REQUEST_URI'])) ? $_SERVER['REQUEST_URI'] : $_SERVER["SCRIPT_FILENAME"])
		);


		$db_cnn = pg_connect($db_cnn_string);


		// we first add the main host to the list (we're already connected to it)
		$hosts_list = [
			("[" . $param_map["DB_HOST"] . "]:" . $param_map["DB_PORT"]) => [
				"address" => $param_map["DB_HOST"],
				"port" => $param_map["DB_PORT"],
				"connection" => &$db_cnn, // this can be by reference as we never manipulate the main connection
				"max_connections" => null
			]
		];

		// do we want data from slaves too?
		if (GET_SLAVE_PROCESSES) {

			// we list the streaming replication slaves
			$rep_sql = "SELECT rep.client_addr, rep.client_port FROM pg_stat_replication AS rep WHERE (rep.state = 'streaming')";
			($rep_slots = pg_query($db_cnn, $rep_sql)) || die("ouch");
			
			while ($rep_slot = pg_fetch_assoc($rep_slots)) {

				$slave_host = strtolower($rep_slot["client_addr"]);
				$slave_port = ((int) $rep_slot["client_port"]);

				$slave_cnn_string = sprintf("host='%1\$s' port=%2\$d user='%3\$s' password='%4\$s' dbname='%5\$s' application_name='%6\$s'",
					$slave_host,
					$param_map["DB_PORT"],
					$param_map["DB_USER"],
					$param_map["DB_PASSWORD"],
					$param_map["DB_NAME"],
					((isset($_SERVER['REQUEST_URI']) && strlen($_SERVER['REQUEST_URI'])) ? $_SERVER['REQUEST_URI'] : $_SERVER["SCRIPT_FILENAME"])
				);
				($slave_cnn = pg_connect($slave_cnn_string)) || die("ouch");

			
				$hosts_list[("[" . $slave_host . "]:" . $param_map["DB_PORT"])] = [
					"address" => $slave_host,
					"port" => $param_map["DB_PORT"],
					"connection" => $slave_cnn,
					"max_connections" => null
				];

			}
			pg_free_result($rep_slots);
		}


		// we now extract more vitals from all the hosts we have a connection for
		foreach ($hosts_list as $host_id => $host_info) {
			($max_usable_connections = pg_fetch_assoc($muc = pg_query($host_info["connection"], $max_connections_sql))) || die("ouch");
			$hosts_list[$host_id]["max_connections"] = ((int) $max_usable_connections["max_connections"]);
			pg_free_result($muc);
		}


		;


		// the processes table is persistent
		$processes_info = [];
		

		// we loop as long as all connections are still ok
		while (! in_array(false, array_map(
			function($host_info) {
				return (isset($host_info["connection"]) && (pg_connection_status($host_info["connection"]) === PGSQL_CONNECTION_OK));
			},
			$hosts_list
		))) {
		
			// list of processess we know exist. Needed to cull dead entries from the list
			$current_process_keys = [];
			
			// this is our sorting index
			$proc_sort = [];

			$states_prompting_alert = [];


			// we now ask all the hosts what they're up to
			foreach ($hosts_list as $host_id => $host_info) {
				($rs_proc = pg_query($host_info["connection"], $processes_sql)) || die("Ouch:\n" . $stats_sql . "\n");


				while ($process_row = pg_fetch_assoc($rs_proc)) {

					// we create a process key, and add it to the list for future clean up
					$current_process_keys[] = ($process_key = ($host_id . "/" . $process_row["session_pid"]));
					
					
					// if this process already exists in the table, we load it into our variable "previous" member
					// otherwise this is just a new, empty array
					// Note that we need to remove some keys to prevent recursive blowup
					isset($processes_info[$process_key]) && ($process_row["previous"] = array_diff_key($processes_info[$process_key], array_flip([
						"loop_stats", "previous"
					])));


					// some paranoid casts. This would blow up on 32 bit systems, but hey, it's 2015
					$process_row["address"] = $host_info["address"];
					$process_row["port"] = $host_info["port"];
					$process_row["session_pid"] = ((int) $process_row["session_pid"]);
					$process_row["epoch"] = ((float) $process_row["epoch"]);
					$process_row["process_utime"] = ((int) $process_row["process_utime"]);
					$process_row["process_stime"] = ((int) $process_row["process_stime"]);
					$process_row["process_rchar"] = ((int) $process_row["process_rchar"]);
					$process_row["process_wchar"] = ((int) $process_row["process_wchar"]);
					$process_row["process_age"] = ((int) $process_row["process_age"]);
					$process_row["statement_age"] = ((int) $process_row["statement_age"]);
					$process_row["blocking_lock_pid"] = (is_null($process_row["blocking_lock_pid"]) ? null : ((int) $process_row["blocking_lock_pid"]));


					// we initialize the time-sensitive metrics (such as CPU load) to 0. Relative rates are normalized from 0.0 to 1.0
					$process_row["loop_stats"] = [
						"cpu_u" => 0.0,
						"cpu_s" => 0.0,
						"io_r" => 0,
						"io_w" => 0
					];
					
					// if there was a "previous loop" for this process, we do all the comparisons and calculate rates
					if (isset($process_row["previous"])) {
						// we pre-calculate the last interval in ticks
						$secs_delta = ($process_row["epoch"] - $process_row["previous"]["epoch"]);
						$ticks_delta = ($secs_delta * ((float) HOST_SC_CLK_TCK));

						$process_row["loop_stats"]["cpu_u"] = (((float) ($process_row["process_utime"] - $process_row["previous"]["process_utime"])) / $ticks_delta);
						$process_row["loop_stats"]["cpu_s"] = (((float) ($process_row["process_stime"] - $process_row["previous"]["process_stime"])) / $ticks_delta);
						$process_row["loop_stats"]["io_r"] = ((float) ($process_row["process_rchar"] - $process_row["previous"]["process_rchar"])) / $secs_delta;
						$process_row["loop_stats"]["io_w"] = ((float) ($process_row["process_wchar"] - $process_row["previous"]["process_wchar"])) / $secs_delta;
					}
					

					// highlight_mode is a bitmask, except for the first 3 bits, which represent the ANSI color (0 through 7)
					$process_row["highlight_mode"] = TERM_COLOR_WHITE; // lines are simple white by default

					// we strip the comment our code prefixes if it's there
					is_null($process_row["query_sql"]) || ($process_row["query_sql"] = preg_replace("/^\\/\\*\\[[0-9]+\\] REQUEST: [^\\/\\*]+\\*\\/ /", "", $process_row["query_sql"]));

					if (! is_null($process_row["blocking_lock_pid"])) {
						// blocked pids are printed in red
						$process_row["highlight_mode"] = (($process_row["highlight_mode"] & (~7)) | TERM_COLOR_RED);
						$states_prompting_alert[] = ("lock=" . $process_row["session_pid"] . "_" . $process_row["blocking_lock_pid"]);
					}

					if ((! is_null($process_row["statement_age"])) && (((int) $process_row["statement_age"]) >= STATEMENT_AGE_ALERT_THRESHOLD)) {
						// long running statements are printed in bold, regardless of the color
						$process_row["highlight_mode"] = ($process_row["highlight_mode"] | TERM_MODE_BOLD);
						// we don't log long running statements if they are of a certain type
							(strtoupper($process_row["user_name"]) != "POSTGRES") &&
							(! preg_match("/^\\s*COPY\\s+/i", $process_row["query_sql"])) &&
							($states_prompting_alert[] = ("slow_statement=" . $process_row["session_pid"] . "_" . $process_row["statement_age"]))
						;
					}

					$processes_info[$process_key] = $process_row;
					

				}
				
				pg_free_result($rs_proc);
			}


			// we now remove processes that are no longer running
			$processes_info = array_intersect_key($processes_info, array_flip($current_process_keys));


			// we populate the sort index
			array_map(
				function($process_key) use($processes_info, &$proc_sort) {
					// here we construct the reverse sort key
					$proc_sort[sprintf("%1\$016.6f:%2\$010d:%3\$012d:%4\$010d:%5\$012d",
						($processes_info[$process_key]["loop_stats"]["cpu_u"] + $processes_info[$process_key]["loop_stats"]["cpu_s"]),
						((($processes_info[$process_key]["blocking_lock_pid"] ?: -1) > 0) ? $processes_info[$process_key]["blocking_lock_pid"] : 0),
						((($processes_info[$process_key]["statement_age"] ?: -1) > 0) ? $processes_info[$process_key]["statement_age"] : 0),
						($processes_info[$process_key]["loop_stats"]["io_r"] + $processes_info[$process_key]["loop_stats"]["io_w"]),
						((($processes_info[$process_key]["process_age"] ?: -1) > 0) ? $processes_info[$process_key]["process_age"] : 0)
					)] = $process_key;
				},
				array_keys($processes_info)
			);

			krsort($proc_sort, SORT_STRING) || die("Couldn't sort processes");


			// the process list is now complete, and ordered



			printf("%s", $clear_scr_seq);



			$header_str = " host            | port  | PID          | user       | database     | application                                          | CPU (U/S)   | I/O (R/W,Kbs) | proc_age   | stmt_age | blocker  | blocked_rel  | SQL";
			(strlen($header_str) < $screen_width) && ($header_str .= str_repeat(" ", ($screen_width - strlen($header_str))));
			count($states_prompting_alert) && ($header_str = ($anomaly_header_seq . $header_str . $term_reset_mode_seq));
			$header_str .= sprintf("\n%1\$s", str_repeat("=", $screen_width));


			printf("%1\$s", $header_str);

			// we only pick what fits in the screen ( through array_slice() )
			foreach (array_slice($proc_sort, 0, ($screen_height - 2), true) as $proc_sort_key => $proc_key) {

				$process_row = $processes_info[$proc_key];
			
	
				$lock_relation_actual_str = (is_null($process_row["lock_relation"]) ? "" : ((string) $process_row["lock_relation"]));
				// we need a one-line version of the SQL statement
				$one_line_sql = (is_null($process_row["query_sql"]) ? "" : preg_replace("/\\s+/ms", " ", $process_row["query_sql"]));
				
	
				$pre_sql_width = strlen($proc_entry_buf = sprintf(" %1\$-15s | %2\$5d | %3\$12s | %4\$-11s| %5\$-13s| %6\$-52s | %7\$5.1f/%8\$5.1f | %9\$6d/%10\$6d | %11\$10d | %12\$8d | %13\$8s | %14\$-13s| ",
					$process_row["address"], // we don't truncate the host. This is very IPV6-unfriendly
					$process_row["port"], // we don't truncate the host. This is very IPV6-unfriendly
					($process_row["session_pid"] . "(" . (is_null($process_row["process_state"]) ? " " : $process_row["process_state"]) . ")"),
					(substr($process_row["user_name"], 0, 10) . ((strlen($process_row["user_name"]) > 10) ? ">" : " ")),
					(substr($process_row["database_name"], 0, 12) . ((strlen($process_row["database_name"]) > 12) ? ">" : " ")),
					substr($process_row["app_name"], 0, 52),
					(is_null($process_row["loop_stats"]["cpu_u"]) ? -1.0 : (100.0 * $process_row["loop_stats"]["cpu_u"])), (is_null($process_row["loop_stats"]["cpu_s"]) ? -1.0 : (100.0 * $process_row["loop_stats"]["cpu_s"])),
					(is_null($process_row["loop_stats"]["io_r"]) ? -1.0 : ((int) ceil($process_row["loop_stats"]["io_r"] / 1024.0))), (is_null($process_row["loop_stats"]["io_w"]) ? -1.0 : ((int) ceil($process_row["loop_stats"]["io_w"] / 1024))),
					$process_row["process_age"],
					$process_row["statement_age"],
					(is_null($process_row["blocking_lock_pid"]) ? "" : ((string) $process_row["blocking_lock_pid"])),
					(substr($lock_relation_actual_str, 0, 12) . ((strlen($lock_relation_actual_str) > 12) ? ">" : " "))
				));


				

				// we set the color from the highlight mode. it is prepended. We also prepend a new line
				$proc_entry_buf = ("\n". $term_colors[7 & $process_row["highlight_mode"]] . $proc_entry_buf);
				// set bold if necessary
				($process_row["highlight_mode"] & TERM_MODE_BOLD) && ($proc_entry_buf = ($term_go_bold . $proc_entry_buf));

				// if we didn't run out of screen space yet, the SQL statement itself needs truncation (or folding if we want multi line)
				if ((MAX_SQL_OUTPUT_LINES > 0) && strlen($one_line_sql)) {
					if (($pre_sql_width < $screen_width)) {

						// determining how much SQL we can fit in what's left of the screen
						$sql_fold_width = ($screen_width - $pre_sql_width);

						// and looping over what's left of the SQL to print the chunks
						for ($sql_fold_id = 0; (($sql_fold_id < ((int) ceil(((float) strlen($one_line_sql)) / ((float) $sql_fold_width)))) && ($sql_fold_id < MAX_SQL_OUTPUT_LINES)); $sql_fold_id++) {
							// if this is not the first output line of the SQL we prepend a newline and the padding
							$sql_fold_id && ($proc_entry_buf .= ("\n". str_repeat(" ", ($pre_sql_width - 2)) . "| "));
							// we then append the current chunk
							$sql_fold = substr($one_line_sql, ($sql_fold_width * $sql_fold_id), $sql_fold_width);
							$proc_entry_buf .= $sql_fold;
							// we pad the line to the screen width
							$proc_entry_buf .= str_repeat(" ", ($screen_width - ($pre_sql_width + strlen($sql_fold))));
						}
					}
				} else {
					// no SQL is being printed. we just pad the line
					$proc_entry_buf .= str_repeat(" ", ($screen_width - $pre_sql_width));
				}
				// we then reset the terminal mode if highlight_mode is we previously set it
				$process_row["highlight_mode"] && ($proc_entry_buf .= $term_reset_mode_seq);
				// and add a newline
				printf("%s", $proc_entry_buf);
				// if we're printing more than one line per process we put out a separator
				(MAX_SQL_OUTPUT_LINES > 1) && printf("%1\$s\n", str_repeat("-", $screen_width));


			}

	
			usleep(1000 * POLL_INTERVAL);
		}

		// we close all connections that are still open
		array_map(
			function($host_info) {
				if (isset($host_info["connection"]) && (pg_connection_status($host_info["connection"]) === PGSQL_CONNECTION_OK)) {
					return pg_close($host_info["connection"]);
				}
			},
			$hosts_list
		);
		
		usleep(1000 * CONNECTION_RETRY_INTERVAL);
	}


?>