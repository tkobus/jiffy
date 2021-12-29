/*
 * Copyright 2021 Poznan University of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

/*
 * This file is available under and governed by the MIT license.
 *
 * Written by Tadeusz Kobus and Maciej Kokocinski, as a modification
 * of java.util.concurrent.ConcurrentSkipListMap.
 *
 * The original implementation of ConcurrentSkipListMap was written by
 * Doug Lea with assistance from members of JCP JSR-166 Expert Group
 * and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package pl.edu.put.concurrent.conctest;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.put.concurrent.jiffy.Jiffy;

public class Main {
	private static final Logger log = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) {
		CommandLineParser parser = new DefaultParser();

		Options options = new Options();
		options.addOption("m", "mode", true,
				"test mode {0,1,2}: 0 (default, short, not even cycle detection), 1 (short), 2 (full)");
		options.addOption("s", "statistics", false, "output detailed statistics");
		options.addOption("t", "threads", true, "number of worker threads");
		
		options.addOption("e", "exectime", true, "exec time (milliseconds)");
		options.addOption("w", "warmuptime", true, "warmup time (milliseconds)");

		options.addOption("h", "help", false, "print this message");
		
		int mode = 0;
		int threads = 16;
		boolean statistics = false;

		int exectime = 500;
		int warmuptime = 5000;
		
		try {
			CommandLine line = parser.parse(options, args);

			if (line.hasOption("mode")) {
				mode = Integer.parseInt(line.getOptionValue("mode"));
				if (mode < 0 || mode > 2)
					throw new ParseException("Mode should be in 0, 1, or 2");
			}
			
			if (line.hasOption("statistics")) {
				statistics = true;
			}
	
			if (line.hasOption("threads")) {
				threads = Integer.parseInt(line.getOptionValue("threads"));
				if (threads <= 0)
					throw new ParseException("Number of threads should be greater than 0");
			}
			
			if (line.hasOption("exectime")) {
				exectime = Integer.parseInt(line.getOptionValue("exectime"));
				if (exectime < 0)
					throw new ParseException("Exec time has to be greater or equal 0");
			}
			
			if (line.hasOption("warmuptime")) {
				warmuptime = Integer.parseInt(line.getOptionValue("warmuptime"));
				if (warmuptime < 0)
					throw new ParseException("Warmup time has to be greater or equal 0");
			}
			
			if (line.hasOption("help")) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("conctest", options);
				System.exit(0);
			}
		} catch (ParseException exp) {
			System.out.println("Unexpected exception: " + exp.getMessage());
			System.exit(1);
		}

		log.info("Running Concurrent Test: mode: {}, threads: {}, statistics: {}", mode, threads, statistics);
		
		ConcurrentMapTest test = new ConcurrentMapTest();

		Jiffy.STATISTICS = statistics;
		test.run(mode, threads, warmuptime, exectime, false);
	}
}
