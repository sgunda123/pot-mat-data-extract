/**
 *
 */
package com.reltio.extract.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.internal.LinkedTreeMap;
import com.reltio.extract.domain.Crosswalk;
import com.reltio.extract.domain.InputAttribute;
import com.reltio.extract.domain.XrefInputAttribute;

/**
 *
 *
 */
public class ExtractServiceUtil {


	public static void createAttributeMapFromProperties(BufferedReader reader,
			Map<String, InputAttribute> attributes) throws IOException {

		boolean eof = false;

		// Read OV attributes File
		while (!eof) {

			String line = reader.readLine();
			Integer noOfValues = 1;

			if (line == null) {
				break;
			} else if (line.contains("=")) {
				line = line.trim();
				String[] attrs = line.split("=", -1);
				if (attrs[1] != null && !attrs[1].isEmpty()) {
					noOfValues = Integer.parseInt(attrs[1]);
				}

				line = attrs[0].trim();
			}
			if (line.contains(".")) {
				String[] nestAttrs1 = line.split("\\.", -1);
				InputAttribute attribute = attributes.get(nestAttrs1[0]);
				if (attribute == null) {
					attribute = new InputAttribute();
				}
				createNestedExtractAttribute(nestAttrs1, 1, attribute,
						noOfValues);
				attributes.put(nestAttrs1[0], attribute);

			} else {
				InputAttribute inputAttribute = new InputAttribute();
				inputAttribute.count = noOfValues;

				attributes.put(line, inputAttribute);
			}

		}
		reader.close();
	}


	private static void createNestedExtractAttribute(String[] attrs,
			Integer index, InputAttribute attribute, Integer noOfValues) {

		String attrName = attrs[index];

		if (attribute.attributesMap == null) {
			attribute.attributesMap = new LinkedHashMap<>();
		}
		InputAttribute nestAttr = attribute.attributesMap.get(attrName);

		if (nestAttr == null) {
			nestAttr = new InputAttribute();
		}

		if (attrs.length > (index + 1)) {
			createNestedExtractAttribute(attrs, index + 1, nestAttr, noOfValues);
		} else {
			nestAttr.count = noOfValues;
		}
		attribute.attributesMap.put(attrName, nestAttr);
	}

	public static void createExtractNestedResponseHeader(
			Map.Entry<String, InputAttribute> nestedAttrs,
			List<String> fileResponseHeader, String headerPrefix) {

		if (headerPrefix != null) {
			headerPrefix += ".";
		} else {
			headerPrefix = "";
		}
		headerPrefix += nestedAttrs.getKey();
		InputAttribute inputAttribute = nestedAttrs.getValue();

		if (inputAttribute.count > 1) {
			for (int i = 1; i <= inputAttribute.count; i++) {
				if (inputAttribute.attributesMap == null) {
					fileResponseHeader.add(headerPrefix + "_" + i);
				} else {
					for (Map.Entry<String, InputAttribute> innerNestedAttrs : inputAttribute.attributesMap
							.entrySet()) {
						createExtractNestedResponseHeader(innerNestedAttrs,
								fileResponseHeader, headerPrefix + "_" + i);
					}
				}
			}
		} else {
			if (inputAttribute.attributesMap == null) {
				fileResponseHeader.add(headerPrefix);
			} else {
				for (Map.Entry<String, InputAttribute> innerNestedAttrs : inputAttribute.attributesMap
						.entrySet()) {
					createExtractNestedResponseHeader(innerNestedAttrs,
							fileResponseHeader, headerPrefix);
				}
			}
		}

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void createExtractOutputData(
			Map.Entry<String, InputAttribute> attribute, List<Object> data,
			Map<String, String> responseMap, String headerPrefix, boolean extractAllValues) {

		if (headerPrefix != null) {
			headerPrefix += ".";
		} else {
			headerPrefix = "";
		}
		headerPrefix += attribute.getKey();
		InputAttribute inputAttribute = attribute.getValue();

		// Simple Attribute
		if (inputAttribute.attributesMap == null) {
			int temp = 1;
			if (data != null && !data.isEmpty()) {

				for (Object obj : data) {
					if (temp > inputAttribute.count) {
						break;
					}
					com.google.gson.internal.LinkedTreeMap object2 = (LinkedTreeMap) obj;
					String value = (String) object2.get("value");
					Boolean ov = (Boolean) object2.get("ov");

					if(extractAllValues)
                    {
                        if(responseMap.get(headerPrefix) != null) {
                            responseMap.put(headerPrefix, responseMap.get(headerPrefix) + " | " + value);
                        }
                        else
                        {
                            responseMap.put(headerPrefix, value);
                        }

                    }
					else if (ov) {
						if (inputAttribute.count > 1) {
							responseMap.put(headerPrefix + "_" + temp++, value);
						} else {
							responseMap.put(headerPrefix, value);
							break;
						}
					}
					//Add logic for nonOV matching
					//else if(nonOVGlobal ==true)
					//responseMap.put(headerPrefix, responseMap.get(headerPrefix)+" | " value);
				}
			}
		} else {
			int temp = 1;
			if (data != null && !data.isEmpty()) {

				for (Object obj : data) {
					if (temp > inputAttribute.count) {
						break;
					}
					com.google.gson.internal.LinkedTreeMap object2 = (LinkedTreeMap) obj;
					Boolean ov = (Boolean) object2.get("ov");
					Map<String, List<Object>> innerAttrs = (Map<String, List<Object>>) object2
							.get("value");

					if (ov) {
						if (inputAttribute.count > 1) {

							for (Map.Entry<String, InputAttribute> inputAttr : inputAttribute.attributesMap
									.entrySet()) {
								List<Object> objects3 = innerAttrs
										.get(inputAttr.getKey());
								createExtractOutputData(inputAttr, objects3,
										responseMap, headerPrefix + "_" + temp, extractAllValues);
							}
						} else {

							for (Map.Entry<String, InputAttribute> inputAttr : inputAttribute.attributesMap
									.entrySet()) {
								List<Object> objects3 = innerAttrs
										.get(inputAttr.getKey());
								createExtractOutputData(inputAttr, objects3,
										responseMap, headerPrefix, extractAllValues);
							}

							break;
						}
						temp++;
					}

				}
			}

		}

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void createUriValuesData(
			Map.Entry<String, InputAttribute> attribute, List<Object> data,
			Map<String, List<XrefInputAttribute>> responseMap) {

		InputAttribute inputAttribute = attribute.getValue();

		// Simple Attribute
		if (inputAttribute.attributesMap == null) {
			if (data != null && !data.isEmpty()) {
				XrefInputAttribute newMapAttr = new XrefInputAttribute();
				newMapAttr.uriValuesMap = new LinkedHashMap<>();
				newMapAttr.count = inputAttribute.count;
				List<XrefInputAttribute> xrefInputAttributes = responseMap
						.get(attribute.getKey());

				if (xrefInputAttributes == null) {
					xrefInputAttributes = new ArrayList<>();
				}
				for (Object obj : data) {
					com.google.gson.internal.LinkedTreeMap object2 = (LinkedTreeMap) obj;
					String value = (String) object2.get("value");
					String uri = (String) object2.get("uri");
					newMapAttr.uriValuesMap.put(uri, value);
				}
				xrefInputAttributes.add(newMapAttr);
				responseMap.put(attribute.getKey(), xrefInputAttributes);

			}
		} else {
			if (data != null && !data.isEmpty()) {
				XrefInputAttribute newMapAttr = null;
				List<XrefInputAttribute> xrefInputAttributes = responseMap
						.get(attribute.getKey());

				if (xrefInputAttributes == null) {
					xrefInputAttributes = new ArrayList<>();
				}
				for (Object obj : data) {
					newMapAttr = new XrefInputAttribute();
					newMapAttr.attributesMap = new LinkedHashMap<>();
					newMapAttr.count = inputAttribute.count;
					com.google.gson.internal.LinkedTreeMap object2 = (LinkedTreeMap) obj;
					Map<String, List<Object>> innerAttrs = (Map<String, List<Object>>) object2
							.get("value");
					Object startObjCross = object2.get("startObjectCrosswalks");

					if (startObjCross != null) {
						List<Object> objects = (List<Object>) startObjCross;
						newMapAttr.isReference = true;
						newMapAttr.refCrosswalks = new ArrayList<>();
						Crosswalk crosswalk = null;

						for (Object crossObj : objects) {
							Map<String, Object> crosswalkValues = (Map<String, Object>) crossObj;
							crosswalk = new Crosswalk();
							String type = (String) crosswalkValues.get("type");
							String value = (String) crosswalkValues
									.get("value");
							crosswalk.type = type;
							crosswalk.value = value;
							newMapAttr.refCrosswalks.add(crosswalk);
						}
					}
					for (Map.Entry<String, InputAttribute> inputAttr : inputAttribute.attributesMap
							.entrySet()) {
						List<Object> objects3 = innerAttrs.get(inputAttr
								.getKey());
						createUriValuesData(inputAttr, objects3,
								newMapAttr.attributesMap);
					}

					xrefInputAttributes.add(newMapAttr);
				}
				responseMap.put(attribute.getKey(), xrefInputAttributes);
			}
		}

	}

	public static void createXrefExtractOutputData(
			Map.Entry<String, List<XrefInputAttribute>> attribute,
			final Crosswalk crosswalk, Map<String, String> responseMap,
			String headerPrefix) {

		if (headerPrefix != null) {
			headerPrefix += ".";
		} else {
			headerPrefix = "";
		}
		headerPrefix += attribute.getKey();
		List<XrefInputAttribute> inputAttributes = attribute.getValue();
		List<String> attributesUrisTobePassed = null;
		if (crosswalk != null) {
			attributesUrisTobePassed = crosswalk.attributes;
		}

		Crosswalk crosswalkToSend = crosswalk;

		if (inputAttributes != null && !inputAttributes.isEmpty()) {
			// Simple Attribute
			if (inputAttributes.size() == 1
					&& inputAttributes.get(0).attributesMap == null) {
				XrefInputAttribute inputAttribute = inputAttributes.get(0);
				if (inputAttribute.uriValuesMap != null) {
					int temp = 1;
					for (Map.Entry<String, String> uriValues : inputAttribute.uriValuesMap
							.entrySet()) {
						if (temp > inputAttribute.count) {
							break;
						}
						if ((attributesUrisTobePassed == null)
								|| (attributesUrisTobePassed != null && attributesUrisTobePassed
										.contains(uriValues.getKey()))) {
							if (inputAttribute.count > 1) {
								responseMap.put(headerPrefix + "_" + temp++,
										uriValues.getValue());
							} else {
								responseMap.put(headerPrefix,
										uriValues.getValue());
								break;
							}
						}
					}

				}
			} else {
				int temp = 1;
				int initalSize = 0;
				for (XrefInputAttribute inputAttribute : inputAttributes) {
					if (temp > inputAttribute.count) {
						break;
					}

					//Checks whether the attribute is reference
					if (inputAttribute.isReference) {
						boolean isRefPartOfSrcCrosswalk = false;
						for (Crosswalk refCrosswalk : inputAttribute.refCrosswalks) {
							//Checks whether reference attribute is part of the current crosswalk
							if (refCrosswalk.type
									.equalsIgnoreCase(crosswalk.type)
									&& refCrosswalk.value
											.equalsIgnoreCase(crosswalk.value)) {
								isRefPartOfSrcCrosswalk = true;
								break;
							}
						}
						if (isRefPartOfSrcCrosswalk) {
							crosswalkToSend = null;
						} else {
							continue;
						}
					}
					initalSize = responseMap.size();

					if (inputAttribute.count > 1) {

						for (Map.Entry<String, List<XrefInputAttribute>> inputAttr : inputAttribute.attributesMap
								.entrySet()) {
							createXrefExtractOutputData(inputAttr,
									crosswalkToSend, responseMap, headerPrefix
											+ "_" + temp);
						}
					} else {

						for (Map.Entry<String, List<XrefInputAttribute>> inputAttr : inputAttribute.attributesMap
								.entrySet()) {
							createXrefExtractOutputData(inputAttr,
									crosswalkToSend, responseMap, headerPrefix);
						}
						if (initalSize < responseMap.size()) {
							break;
						}
					}

					if (initalSize < responseMap.size()) {
						temp++;
					}

				}

			}

		}

	}

}
