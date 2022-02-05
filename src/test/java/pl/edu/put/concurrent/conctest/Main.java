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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Cleanup;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.put.concurrent.jiffy.Jiffy;

import java.io.*;
import java.nio.file.Paths;
import java.util.List;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class Main {
	private static final Logger log = LoggerFactory.getLogger(Main.class);

	private static final String MODE_OPTION = "mode";

	private static final String STATISTICS_OPTION = "statistics";
	private static final String THREADS_OPTION = "threads";

	private static final String EXECTIME_OPTION = "exectime";
	private static final String WARMUPTIME_OPTION = "warmuptime";

	private static final String OUTPUT_FILENAME_OPTION = "output";
	private static final String INPUT_FILENAME_OPTION = "input";

	private static final String HELP_OPTION = "help";

	private int mode = 0;
	private int threads = 16;
	private boolean statistics = false;

	private int exectime = 500;
	private int warmuptime = 5000;

	private String outputFile = null;
	private String inputFile = null;

	public static void main(String[] args) {
		Main main = new Main();
		main.run(args);
	}

	void run(String[] args) {
		CommandLineParser parser = new DefaultParser();
		Options options = addOptions();

		try {
			parseOptions(args, parser, options);
		} catch (ParseException exp) {
			System.out.println("Unexpected exception: " + exp.getMessage());
			System.exit(1);
		}

		List<Trace> traces;
		if (inputFile == null) {
			log.info("Running Concurrent Test: mode: {}, threads: {}, statistics: {}", mode, threads, statistics);
			traces = runTests();
		} else {
			traces = readTraces();
		}

		if (outputFile != null) {
			saveTraces(traces);
			return;
		}

		runChecker(traces);

		System.out.println("Finished.");
	}

	private static ObjectMapper objectMapper() {
		return new ObjectMapper()
				.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
				.setSerializationInclusion(JsonInclude.Include.NON_NULL);
	}

	private void saveTraces(List<Trace> traces) {
		log.info("Saving traces in {}", outputFile);

		ObjectMapper objectMapper = objectMapper();
		try {
			objectMapper.writeValue(Paths.get(outputFile).toFile(), traces);
		} catch (IOException e) {
			log.error("Could not save traces to file {}, msg: {}", outputFile, e.getMessage());
			e.printStackTrace();
		}
	}

	private List<Trace> readTraces() {
		log.info("Reading traces from {}", inputFile);

		ObjectMapper objectMapper = objectMapper();
		try {
			return (List<Trace>)objectMapper.readValue(Paths.get(inputFile).toFile(), List.class);
		} catch (IOException e) {
			log.error("Could not read traces from file {}, msg: {}", inputFile, e.getMessage());
//			e.printStackTrace();
			return null;
		}
	}


	private void runChecker(List<Trace> traces) {
		TraceChecker checker = new TraceChecker(traces);
		checker.run(TraceChecker.Mode.getMode(mode));
	}

	private Options addOptions() {
		Options options = new Options();
		options.addOption("m", MODE_OPTION, true,
				"test mode {0,1,2}: 0 (default, short, not even cycle detection), 1 (short), 2 (full)");
		options.addOption("s", STATISTICS_OPTION, false, "output detailed statistics");
		options.addOption("t", THREADS_OPTION, true, "number of worker threads");

		options.addOption("e", EXECTIME_OPTION, true, "exec time (milliseconds)");
		options.addOption("w", WARMUPTIME_OPTION, true, "warmup time (milliseconds)");

//		options.addOption("o", OUTPUT_FILENAME_OPTION, true, "name of the output file with traces");
//		options.addOption("i", INPUT_FILENAME_OPTION, true, "name of the file with traces");

		options.addOption("h", HELP_OPTION, false, "print this message");
		return options;
	}

	private void parseOptions(String[] args, CommandLineParser parser, Options options) throws ParseException {
		CommandLine line = parser.parse(options, args);

		if (line.hasOption(MODE_OPTION)) {
			mode = Integer.parseInt(line.getOptionValue(MODE_OPTION));
			if (mode < 0 || mode > 2)
				throw new ParseException("Mode should be in 0, 1, or 2");
		}

		if (line.hasOption(STATISTICS_OPTION)) {
			statistics = true;
		}

		if (line.hasOption(THREADS_OPTION)) {
			threads = Integer.parseInt(line.getOptionValue(THREADS_OPTION));
			if (threads <= 0)
				throw new ParseException("Number of threads should be greater than 0");
		}

		if (line.hasOption(EXECTIME_OPTION)) {
			exectime = Integer.parseInt(line.getOptionValue(EXECTIME_OPTION));
			if (exectime < 0)
				throw new ParseException("Exec time has to be greater or equal 0");
		}

		if (line.hasOption(WARMUPTIME_OPTION)) {
			warmuptime = Integer.parseInt(line.getOptionValue(WARMUPTIME_OPTION));
			if (warmuptime < 0)
				throw new ParseException("Warmup time has to be greater or equal 0");
		}

		if (line.hasOption(OUTPUT_FILENAME_OPTION)) {
			outputFile = line.getOptionValue(OUTPUT_FILENAME_OPTION);
			if (isBlank(outputFile)) {
				throw new IllegalArgumentException("Output filename cannot be empty");
			}
		}

		if (line.hasOption(INPUT_FILENAME_OPTION)) {
			inputFile = line.getOptionValue(INPUT_FILENAME_OPTION);
			if (isBlank(inputFile)) {
				throw new IllegalArgumentException("Input filename cannot be empty");
			}
		}

		if (line.hasOption(HELP_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("conctest", options);
			System.exit(0);
		}
	}

	private List<Trace> runTests() {
		ConcurrentMapTest test = new ConcurrentMapTest();
		Jiffy.STATISTICS = statistics;
		return test.run(mode, threads, warmuptime, exectime, false);
	}
}
