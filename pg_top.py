#!/usr/bin/python3 -uB

import sys, os, curses, time, atexit, signal, re


print({n: idx for (idx, n) in enumerate([
	"NONE",
	"ALIGN_LEFT", "ALIGN_CENTER", "ALIGN_RIGHT",
	# the following are straight from curses
	"BLINK", "BOLD", "DIM", "REVERSE", "STANDOUT", "UNDERLINE"
])})

print(1 << 19)
print(curses.COLOR_BLUE)
sys.exit(0)



def cleanup_and_exit():
	global main_window

	try:
		curses.nocbreak()
		main_window.keypad(False)
		curses.echo()
		curses.endwin()
	except Exception as e_curses:
		# curses errors are acceptable here
		if (not (hasattr(e_curses, "__module__") and ("CURSES" in e_curses.__module__.upper()))):
			raise

	os._exit(0)

atexit.register(cleanup_and_exit)





def term_rescale():
	global main_window
	#print(max_y, max_x)


def signal_handler(sigspec, frame):
	if (sigspec == signal.SIGWINCH):
		curses.ungetch(curses.KEY_RESIZE)
	else:
		cleanup_and_exit()



for sigcode in [ "HUP", "INT", "QUIT", "TERM" ]:
	signal.signal(signal.__dict__["SIG" + sigcode], signal_handler)


def rescale_window(trash):
	global main_window
	(max_y, max_x) = main_window.getmaxyx()
	main_window.clear()
	curses.resizeterm(max_y, max_x)



def get_proc_info():
	"""
		Returns a simplified process table for testing purposes
		
		The table is in the form of a dictionary of tuples, indexed by pid.
		Each tuple is strictured as follows:
		
		0:			(list) command line
		1:			(int) parent pid
		2:			(dict) dictionary of environemnt variables
		3:			(str)status letter
	"""

	out_proc = {}
	for proc_pid in map(int, filter(lambda d: re.search(pattern = "^[0-9]+$", string = d), os.listdir("/proc"))):

		# we cook the whole thing into a list first
		proc_info = []

		# fabricated map files to extract the info from based on what field we're
		# producing. A name-indexed cache  dictionary keeps a copy of the files we've
		# already been through
		cached_files = {}
		for (fld_id, proc_file) in enumerate([
			"cmdline", "stat", "environ", "stat"
		]):


			cache_key = "%d_%s" % (proc_pid, proc_file)

			# is it in cache
			if (cache_key not in cached_files):
				
				# nope.
				# read & clean & decode
				try:
					proc_fh = open("/proc/%d/%s" % (proc_pid, proc_file), "rb")
				except (PermissionError) as e_open:
					# meh
					continue
				f_buf = proc_fh.read()
				proc_fh.close()

				# We have to massage some files to make them friendly to nullchar-separation
				
				# /proc/$$/stat might contain white spaces in between the brackets
				# (or additional brackets), so some re magic is needed
				if (proc_file == "stat"):
					#print(f_buf)
					(pid, name, others) = map(bytes.strip, re.split(pattern = b"[()]+", string = f_buf))
					f_buf = b"\0".join([pid, name] + re.split(pattern = b"\\s+", string = others))

				cached_files[cache_key] = [ f.decode(encoding = "ASCII", errors = "replace") for f in f_buf.split(sep = b"\0") ]

			f_fields = cached_files[cache_key]


			# different treatment for env and cmdline
			if (fld_id == 0):
				# we clean up some empty command line argument at the tail end
				proc_argv = list(f_fields)
				while (len(proc_argv) and (not(len(proc_argv[-1])))):
					del(proc_argv[-1])
				proc_info.append(proc_argv)
			elif (fld_id == 1):
				proc_info.append(f_fields[3])
			elif (fld_id == 2):
				# sometimes processes screw up their own environemnt vector and create strings without "="
				env_vars = {}
				for (env_n, env_v) in (f.split(sep = "=", maxsplit = 1) for f in filter(lambda f : ("=" in f), f_fields)):
					env_vars[env_n] = env_v
				proc_info.append(env_vars)

		out_proc[proc_pid] = tuple(proc_info)

	return out_proc



class ColumnDefinition():
	"""
		Definition of a table column. A RowDefinition is basically a tuple
		of many of these
	"""
	def __init__(self, 
		label,
		width = 12,
		decimal_places = 3,
		header_format = FORMAT_ALIGN_CENTER,
		default_data_format = 0,
		
	):
		"""
			Standard initializer, sets the properties of a given column.
			
			Args:
				label:				(str)The column label
				width:				(int)Formatting width of the column.
				decimal_places:		(int)How many decimal digits to print for floats. Note that decimal
									digits and the dot count toward the total width
				header_format:		(int)Bitmask representing 
		"""
sys.exit(0)



def tabulize(
	column_defs,
	records,
	formatters = {},
	col_separator = " | ",
	row_separator = None,
	header_separator = "-"
):
	"""
		Accepts a table column definition and 2 dimensional array of values,
		and returns a data grid string that can be printed.
		Each element is formatted according to its data type and the definition
		specs (eg: text does line-wrap but numbers don't and throw an error instead)
		
		Args:
		
			column_defs:		(list)	a list of 4-tuples defining each column and its property. Each member
										It is defined by:
											0) Column header
											1)	Column width (separator not included). Strings
												in eccess of this length will be wrapped down, other types will throw ValueError.
												This value (minus one for the dot) is also used as precision for float representation.
												A negative value indicates that strings should be truncated and not wrapped to the next
												line.
												Column headers will always be centered
											2)	Flags the data within the column. The default is type dependent (left for strings,
												right for numbers)
											3)	Scale of float representation


			records:			(list)	A list of tuples defining the input tabular data.
										If the number of members in any of the records is
										different than that of column definitions, a
										ValueError is thrown.
			formatters:			(dict)	Map of format modifiers for the rows/data cells in the data.
										Members are indexed by row index in "records", 
									
			col_separator:		(str)	Charater string between columns. It is the caller's responsibility to add some spacing in there
			row_separator:		(str)	Character string that is used to draw lines between rows of data. Passing None skips the inter-row separator altoghether
			header_separator:
			


		Example of a simplified process table definition:

		formatted_table = tabulize(
			column_defs = [
				("PID",                 8,          FORMAT_ALIGN_RIGHT,                None),
				("Command line",        32,         FORMAT_ALIGN_LEFT,                 None),
				("ENV",                 64,         FORMAT_ALIGN_LEFT,                 None)
			],
			records = [
				(1,           "init",         ""),
				(5121,        "bash",         "PATH=/usr/local/bin:/usr/bin:/bin")
			]
		)

		Returns:
			a formatted version of "records" (a list of strings

	"""
	
	# some validation first
	if (not isinstance(column_defs, list)):
		raise TypeError("column_defs must be a list")

	for (col_def_id, col_def_flds) in enumerate(column_defs):
		if (not isinstance(col_def, tuple)):
			raise TypeError("column defintion member %d is of type  %s (should be a tuple)" % (col_def_id, col_def_flds.__class__.__name__))
	
	sys.exit(0)


	if (not isinstance(column_defs, list)):
		raise TypeError("column_defs must be a list")
	
	# we're breaking into multi-lines. We can't output anything until we looped.
	# This 2 dimensional array represents the lines
	pool_row_grid = []

	for (col_id, col_def) in enumerate(column_defs):

		if (col_id >= len(datum_line)):
			break

		col_width = len(col_def[0])
		col_datum = datum_line[col_id]


		# we first sanitize the data and pre-format it
		decimals = None
		if (isinstance(col_datum, (int, float))):
			if (isinstance(col_datum, float)):
				decimals = col_def[2] or 6 # this is quite arbitrary
			else:
				decimals = 0
			datum_out_buf = ("%%.%df" % decimals) % float(col_datum)
		else:
			datum_out_buf = str(col_datum.strip())

		# overflowing numbers are invalidated
		if ((len(datum_out_buf) > col_width) and (decimals is not None)):
			datum_out_buf = "#" * col_width


		# Now we determine how many lines we need and prepare an array which will
		# be our pool entry status report row (which could me multi-line)
		# we need to break lines that are too long into smaller chunks
		actual_lines = []
		for dl in map(str.strip, datum_out_buf.split("\n")):
			actual_lines += filter(len, [ dl[offset:offset + col_width].strip() for offset in range(0, len(dl), col_width) ])

		# This involves breaking down all cells into width-sized chunks
		# break it into its length
		for (datum_line_id, datum_line_str) in enumerate(actual_lines):
			if (datum_line_id >= len(pool_row_grid)):
				# we initialize all lines as empty padded if the sub-row does not exist
				pool_row_grid.append([ " " * len(cd[0]) for cd in column_defs ])

			# we coose an appropriate justification, defaulting to
			# left only for strings
			j_func = {
				FORMAT_ALIGN_LEFT: str.ljust,
				FORMAT_ALIGN_RIGHT: str.ljust,
				FORMAT_ALIGN_CENTER: str.center
			}.get(col_def[1])
			if (j_func is None):
				j_func = (str.ljust if isinstance(col_datum, str) else str.rjust)

			pool_row_grid[datum_line_id][col_id] = j_func(datum_line_str, col_width)

			#pool_row_grid.app
			#tmp_dl.extend([ datum_line[co:co + col_width] for co in range(0, len(datum_line), col_width) ])
			

	if (len(pool_row_grid)):
		rep_lines.append(pool_row_grid)





if (__name__ == "__main__"):

	# columns definition tuples. Each one is defined by
	# - header name
	# - column width (separator not included). Strings in eccess of this length will be wrapped down, other types will throw ValueError
	#   this value (minus one for the dot) is also used as precision for floats
	# - padding charactrer. Defaults to " "
	# - data alignment. The default depends on data type if this is left to None. (header is always centered)
	# - scale of float representations. Cannot be bigger


	column_defs = [
		("Pool PID",			8,			FORMAT_ALIGN_RIGHT,				None),
		("Command line", 		32,			FORMAT_ALIGN_LEFT,					None),
		("ENV",					64,			FORMAT_ALIGN_LEFT,					None)
	]


	import pprint
	pprint.pprint(get_proc_info())

	#def render_table(column_defs, t_data, row_sep, c_sep, 
	#)


	#print("%0.0f" % 0.0)
	sys.exit(0)

	for (pool_pid, pool_info) in pools.items():
		#(start_time, connection_time, pool_id, backend_id, backend_pid, user, db, backend_socket_inode, frontend_socket_inode)


		backends_str = ""
		connect_tss = ""
		for (be_id, be_info) in pool_info[2].items():
			lf = "\n" if be_id else ""

			backends_str += "%sP%d/B%d> %s:%d" % (( lf, ) + ( be_info[1], be_info[2] ) + backends[be_info[2]][1:3])
			#datum_line.append(time.strftime(PGPOOL_TS_FORMAT, time.gmtime(be_info[0])))
			
		datum_line = [
			pool_pid,
			time.strftime(PGPOOL_TS_FORMAT, time.gmtime(pool_info[0])),
			backends_str
		]



		# we're breaking into multi-lines. We can't output anything until we looped.
		# This 2 dimensional array represents the lines
		pool_row_grid = []

		for (col_id, col_def) in enumerate(column_defs):

			if (col_id >= len(datum_line)):
				break

			col_width = len(col_def[0])
			col_datum = datum_line[col_id]


			# we first sanitize the data and pre-format it
			decimals = None
			if (isinstance(col_datum, (int, float))):
				if (isinstance(col_datum, float)):
					decimals = col_def[2] or 6 # this is quite arbitrary
				else:
					decimals = 0
				datum_out_buf = ("%%.%df" % decimals) % float(col_datum)
			else:
				datum_out_buf = str(col_datum.strip())

			# overflowing numbers are invalidated
			if ((len(datum_out_buf) > col_width) and (decimals is not None)):
				datum_out_buf = "#" * col_width


			# Now we determine how many lines we need and prepare an array which will
			# be our pool entry status report row (which could me multi-line)
			# we need to break lines that are too long into smaller chunks
			actual_lines = []
			for dl in map(str.strip, datum_out_buf.split("\n")):
				actual_lines += filter(len, [ dl[offset:offset + col_width].strip() for offset in range(0, len(dl), col_width) ])

			# This involves breaking down all cells into width-sized chunks
			# break it into its length
			for (datum_line_id, datum_line_str) in enumerate(actual_lines):
				if (datum_line_id >= len(pool_row_grid)):
					# we initialize all lines as empty padded if the sub-row does not exist
					pool_row_grid.append([ " " * len(cd[0]) for cd in column_defs ])

				# we coose an appropriate justification, defaulting to
				# left only for strings
				j_func = {
					FORMAT_ALIGN_LEFT: str.ljust,
					FORMAT_ALIGN_RIGHT: str.ljust,
					FORMAT_ALIGN_CENTER: str.center
				}.get(col_def[1])
				if (j_func is None):
					j_func = (str.ljust if isinstance(col_datum, str) else str.rjust)

				pool_row_grid[datum_line_id][col_id] = j_func(datum_line_str, col_width)

				#pool_row_grid.app
				#tmp_dl.extend([ datum_line[co:co + col_width] for co in range(0, len(datum_line), col_width) ])
				

		if (len(pool_row_grid)):
			rep_lines.append(pool_row_grid)
		


	sys.exit(0)


	try:
		main_window = curses.initscr()
		curses.start_color()
		curses.noecho()
		curses.cbreak()
		main_window.border()
		while (True):
			next_input = main_window.getch()
			next_action = {
				curses.KEY_RESIZE: rescale_window
			}.get(next_input)
			if (next_action is not None):
				next_action(*([next_input] if (next_action.__code__.co_argcount) else []))
			else:
				main_window.addstr(0, 0, "Last typed: %s" % (chr(next_input)), curses.A_REVERSE)
		main_window.refresh()
		#curses.refresh()
	except Exception as e_curses:
			#sys.stderr.write("Curses error: %s(%s)\n" % (e_curses.__class__.__name__, e_curses))
		if (hasattr(e_curses, "__module__") and ("CURSES" in e_curses.__module__.upper())):
			if (os.system("xfce4-terminal -x bash -c '%s; sleep 100000000'" % sys.argv[0])):
				sys.stderr.write("Curses error: %s(%s)\n" % (e_curses.__class__.__name__, e_curses))
				sys.exit(4)
			sys.exit(0)
		raise






