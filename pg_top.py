#!/usr/bin/python3 -uB

import sys, os, curses, time, atexit, signal


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
		exit(0)
	raise



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

	for (col_id, col_def) in enumerate(columns):


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
				pool_row_grid.append([ " " * len(cd[0]) for cd in columns ])

			# we coose an appropriate justification, defaulting to
			# left only for strings
			j_func = {
				ALIGN_LEFT: str.ljust,
				ALIGN_RIGHT: str.ljust,
				ALIGN_CENTER: str.center
			}.get(col_def[1])
			if (j_func is None):
				j_func = (str.ljust if isinstance(col_datum, str) else str.rjust)

			pool_row_grid[datum_line_id][col_id] = j_func(datum_line_str, col_width)

			#pool_row_grid.app
			#tmp_dl.extend([ datum_line[co:co + col_width] for co in range(0, len(datum_line), col_width) ])
			

	if (len(pool_row_grid)):
		rep_lines.append(pool_row_grid)
	


