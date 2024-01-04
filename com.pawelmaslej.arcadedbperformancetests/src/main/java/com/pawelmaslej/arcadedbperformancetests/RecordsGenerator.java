package com.pawelmaslej.arcadedbperformancetests;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

/**
 * @author Pawel Maslej
 * @since 3 Jan 2024
 */
public class RecordsGenerator {

	static int recordsNo = 10000;

	public static void main(String [] args) throws FileNotFoundException, IOException {
		var installDir = Paths.get("").toAbsolutePath().toFile();
		var testDataDir = new File(installDir, "testdata");
		testDataDir.mkdir();
		var file = new File(testDataDir, "newdata_" + recordsNo);

		try (var os = new BufferedOutputStream(new FileOutputStream(file))) {
			for (int i=0; i<recordsNo; i++) {
				var json =
					"""
					{
					    "glossary": {
					        "title": "example record",
							"RecordDiv": {
					            "title": "R",
								"RecordList": {
					                "RecordEntry": {
					                    "ID": "Record",
										"SortAs": "Name",
										"RecordTerm": "Standard Generalized Markup Language",
										"Acronym": "SGML",
										"Abbrev": "ISO 8879:1986",
										"RecordDef": {
					                        "para": "A meta-markup language, used to create markup languages such as DocBook.",
											"RecordSeeAlso": ["GML", "XML"]
					                    },
										"RecordSee": "markup"
					                }
					            }
					        }
					    }
					}""";
				json = json.replaceAll("\n", "").replaceAll("\r", "");
				var text = json + ((i+1) < recordsNo ? "\n" : "");
				os.write(text.getBytes(StandardCharsets.UTF_8));
			}
			os.flush();
		}

		System.out.println("Test data created at: " + file.getAbsolutePath());
	}
}