package com.pawelmaslej.arcadedbperformancetests.utils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author Pawel
 * @since 22 Feb 2023
 */
public class Utils
{
	private static final String SEPARATOR_2 = "_!s!_";

	public static void delete(File f) throws IOException
	{
		if (f.exists()) {
			var listFiles = f.listFiles();
			if (listFiles != null) {
				for (var cf : f.listFiles()) {
					delete(cf);
				}
			}
			Files.delete(f.toPath());
		}
	}

	public static boolean waitForCondition(final Supplier<Boolean> predicate, final int wait, final TimeUnit unit, final int pauseTime)
	{
		long start = System.currentTimeMillis();
		final long deadline = start + TimeUnit.MILLISECONDS.convert(wait, unit);
		boolean result = predicate.get().booleanValue();
		while (!result)
		{
			start = System.currentTimeMillis();
			if (start >= deadline)
			{
				break;
			}
			try
			{
				Thread.sleep(pauseTime);
			}
			catch (InterruptedException e)
			{
			}
			result = predicate.get().booleanValue();
		}

		return result;
	}

	/** Encodes text as bytes using UTF-8 and coverts it to String using Base64. */
	public static String toBase64(String text)
	{
		return Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8));
	}

	/** Decodes Base64 text to bytes and creates text String from it using UTF-8. */
	public static String fromBase64(String base64Text)
	{
		return new String(Base64.getDecoder().decode(base64Text), StandardCharsets.UTF_8);
	}

	public static String listToString(List<String> strings) {
		StringBuilder sb = new StringBuilder();
		for (int i=0; i<strings.size(); i++) {
			if (i > 0) {
				sb.append(SEPARATOR_2);
			}
			sb.append(strings.get(i));
		}
		return sb.toString();
	}

	public static List<String> listFromString(String text)
	{
		List<String> list;
		if (text.isEmpty()) {
			list = new ArrayList<>(0);
		} else {
			String[] values = text.split(SEPARATOR_2);
			list = new ArrayList<String>(Arrays.asList(values));
		}
		return list;
	}
}