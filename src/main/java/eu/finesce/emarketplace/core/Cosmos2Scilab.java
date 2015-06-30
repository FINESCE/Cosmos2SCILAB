/*
 * (C) Copyright 2014 FINESCE-WP4.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package eu.finesce.emarketplace.core;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.TimerTask;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hsqldb.util.CSVWriter;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import com.csvreader.CsvWriter;

import eu.finesce.emarketplace.domain.HistLoadDataList;
import eu.finesce.emarketplace.domain.Load;
//import eu.finesce.emarketplace.context.TotConsProdContextElement;
import eu.finesce.emarketplace.domain.TotConsProd;
import eu.finesce.emarketplace.domain.WeatherForecast;

public class Cosmos2Scilab extends TimerTask {

	private static final Log logger = LogFactory.getLog(Cosmos2Scilab.class);
	List<HistLoadDataList> loadDataList = null;
	Properties prop;
	TotConsProd consprod = new TotConsProd();
	private String mappingFileName;
	protected String weatherRestUrl, weatherAppPath, loadRestUrl, loadAppPath,
			weatherCsvFile, loadCsvFile;
	protected long sampleDateParam = 0;

	public Cosmos2Scilab(String mappingFileName) {
		this.mappingFileName = mappingFileName;
	}

	@Override
	public void run() {
		exec(mappingFileName);
	}

	public void exec(String mappingFileName) {
		this.getPropertiesData(mappingFileName);
		List<Load> listLoads = new ArrayList<Load>();
		
		try {
			listLoads = retrieveLoadListFromResponse();
		} catch (Exception e) {
			logger.error(
					"Oopssss!!!!!! There was a problem in exec method during retrieving the listLoads !!!",
					e);
		}
		
		if (listLoads != null && !listLoads.isEmpty()){
			// write the load csv file
			try {
				//writeLoadCsv(retrieveLoadListFromResponse());
				writeLoadCsv(listLoads);
				logger.info("Load file written at " + new Date().toString());
			} catch (Exception e) {
				logger.error(
						"Oopssss!!!!!! There was a problem to write Load csv file, i try later!!!",
						e);
			}
	
			// write the weather csv file
			try {
				writeWeatherCsv(retrieveWeatherFromResponse());
				logger.info("Weather file written at " + new Date().toString());
			} catch (Exception e) {
				logger.error(
						"Oopssss!!!!!! There was a problem to write Weather csv file, i try later!!!",
						e);
			}
		}else{
		   logger.info("Oopssss!!!There was a problem to retrieve Load data from Cosmos! No files will be written at the moment!");
		}
	}

	/**
	 * Method to retrieve properties data from NAME.properties
	 * 
	 * @param mappingFileName
	 * @return
	 */
	public void getPropertiesData(String mappingFileName) {
		prop = new Properties();

		try {
			prop.load(this.getClass().getClassLoader()
					.getResourceAsStream(mappingFileName));
			weatherRestUrl = prop.getProperty("cosmos2scilab.weatherRestUrl");
			weatherAppPath = prop.getProperty("cosmos2scilab.weatherAppPath");
			loadRestUrl = prop.getProperty("cosmos2scilab.loadRestUrl");
			loadAppPath = prop.getProperty("cosmos2scilab.loadAppPath");
			weatherCsvFile = prop.getProperty("cosmos2scilab.wheterCsvFile");
			loadCsvFile = prop.getProperty("cosmos2scilab.loadCsvFile");
		} catch (IOException e) {
			logger.error("Error during get properties data by:"
					+ mappingFileName, e);
		}
	}

	public List<Load> retrieveLoadListFromResponse() {

		Response rs = getLastLoadData();
		String fileResponse = new String(rs.readEntity(String.class));
		SAXBuilder builder = new SAXBuilder();
		List<Load> listLoad = new ArrayList<Load>();
		try {
			Document document = builder.build(new StringReader(fileResponse));
			// Get xml root
			Element rootElement = document.getRootElement();

			// get root's childs
			List<?> children = rootElement.getChildren();
			Iterator<?> iterator = children.iterator();
			while (iterator.hasNext()) {
				Load load = new Load();
				Element item = (Element) iterator.next();
				if ("LoadDataList".equals(item.getName())) {
					load.setMeterId(item.getChild("meterID").getText());
					load.setLoadSampleDate(Long.valueOf(item.getChild(
							"loadSampleDate").getText()));
					load.setSampleNumber(Integer.valueOf(item.getChild(
							"sampleNumber").getText()));
					load.setUpstreamActivePowerEUA(Double.valueOf(item
							.getChild("upstreamActivePowerEUA").getText()));
					load.setReactiveInductivePowerEEI(Double.valueOf(item
							.getChild("reactiveInductivePowerEEI").getText()));
					load.setReactiveCapacitivePowerEEC(Double.valueOf(item
							.getChild("reactiveCapacitivePowerEEC").getText()));
					load.setDownstreamActivePowerEEA(Double.valueOf(item
							.getChild("downstreamActivePowerEEA").getText()));
					load.setReactiveInductivePowerEUI(Double.valueOf(item
							.getChild("reactiveInductivePowerEUI").getText()));
					load.setReactiveCapacitivePowerEUC(Double.valueOf(item
							.getChild("reactiveCapacitivePowerEUC").getText()));
					listLoad.add(load);
					// param for weather
					sampleDateParam = load.getLoadSampleDate();
				}
			}
		} catch (JDOMException e) {
			logger.error("Error reading xml file :", e);
		} catch (IOException e) {
			logger.error("Error openinig xml (Rest) file :", e);
		}

		return listLoad;
	}

	public WeatherForecast retrieveWeatherFromResponse() {

		Response rs = getLastWeatherData(sampleDateParam);
		String fileResponse = new String(rs.readEntity(String.class));
		SAXBuilder builder = new SAXBuilder();
		WeatherForecast weather = new WeatherForecast();
		// List<Load> listLoad = new ArrayList<Load>();
		try {
			Document document = builder.build(new StringReader(fileResponse));
			// Get xml root
			Element rootElement = document.getRootElement();

			weather.setCurrentTime(Long.valueOf(rootElement.getChild(
					"currentTime").getText()));
			weather.setDailySunriseTime(Long.valueOf(rootElement.getChild(
					"dailySunriseTime").getText()));
			weather.setDailySunsetTime(Long.valueOf(rootElement.getChild(
					"dailySunsetTime").getText()));
			weather.setTemperatureMin(Double.valueOf(rootElement.getChild(
					"temperatureMin").getText()));
			weather.setTemperatureMax(Double.valueOf(rootElement.getChild(
					"temperatureMax").getText()));
			weather.setCurrentTemperature(Double.valueOf(rootElement.getChild(
					"currentTemperature").getText()));

			weather.setAfter1hTemperature(Double.valueOf(rootElement.getChild(
					"after1hTemperature").getText()));
			weather.setAfter1hCloudCover(Double.valueOf(rootElement.getChild(
					"after1hCloudCover").getText()));
			weather.setAfter1hPrecipIntensity(Double.valueOf(rootElement
					.getChild("after1hPrecipIntensity").getText()));
			weather.setAfter1hPrecipProbability(Double.valueOf(rootElement
					.getChild("after1hPrecipProbability").getText()));

			weather.setAfter3hTemperature(Double.valueOf(rootElement.getChild(
					"after3hTemperature").getText()));
			weather.setAfter3hCloudCover(Double.valueOf(rootElement.getChild(
					"after3hCloudCover").getText()));
			weather.setAfter3hPrecipIntensity(Double.valueOf(rootElement
					.getChild("after3hPrecipIntensity").getText()));
			weather.setAfter3hPrecipProbability(Double.valueOf(rootElement
					.getChild("after3hPrecipProbability").getText()));

			weather.setAfter6hTemperature(Double.valueOf(rootElement.getChild(
					"after6hTemperature").getText()));
			weather.setAfter6hCloudCover(Double.valueOf(rootElement.getChild(
					"after6hCloudCover").getText()));
			weather.setAfter6hPrecipIntensity(Double.valueOf(rootElement
					.getChild("after6hPrecipIntensity").getText()));
			weather.setAfter6hPrecipProbability(Double.valueOf(rootElement
					.getChild("after6hPrecipProbability").getText()));

			weather.setAfter12hTemperature(Double.valueOf(rootElement.getChild(
					"after12hTemperature").getText()));
			weather.setAfter12hCloudCover(Double.valueOf(rootElement.getChild(
					"after12hCloudCover").getText()));
			weather.setAfter12hPrecipIntensity(Double.valueOf(rootElement
					.getChild("after12hPrecipIntensity").getText()));
			weather.setAfter12hPrecipProbability(Double.valueOf(rootElement
					.getChild("after12hPrecipProbability").getText()));

			weather.setAfter24hTemperature(Double.valueOf(rootElement.getChild(
					"after24hTemperature").getText()));
			weather.setAfter24hCloudCover(Double.valueOf(rootElement.getChild(
					"after24hCloudCover").getText()));
			weather.setAfter24hPrecipIntensity(Double.valueOf(rootElement
					.getChild("after24hPrecipIntensity").getText()));
			weather.setAfter24hPrecipProbability(Double.valueOf(rootElement
					.getChild("after24hPrecipProbability").getText()));
			
			//New fields for predictions
			weather.setAfter2hTemperature(Double.valueOf(rootElement.getChild("after2hTemperature").getText()));
			weather.setAfter2hCloudCover(Double.valueOf(rootElement.getChild("after2hCloudCover").getText()));
			weather.setAfter2hPrecipIntensity(Double.valueOf(rootElement.getChild("after2hPrecipIntensity").getText()));
			weather.setAfter2hPrecipProbability(Double.valueOf(rootElement.getChild("after2hPrecipProbability").getText()));

			weather.setAfter4hTemperature(Double.valueOf(rootElement.getChild("after4hTemperature").getText()));
			weather.setAfter4hCloudCover(Double.valueOf(rootElement.getChild("after4hCloudCover").getText()));
			weather.setAfter4hPrecipIntensity(Double.valueOf(rootElement.getChild("after4hPrecipIntensity").getText()));
			weather.setAfter4hPrecipProbability(Double.valueOf(rootElement.getChild("after4hPrecipProbability").getText()));

			weather.setAfter5hTemperature(Double.valueOf(rootElement.getChild("after5hTemperature").getText()));
			weather.setAfter5hCloudCover(Double.valueOf(rootElement.getChild("after5hCloudCover").getText()));
			weather.setAfter5hPrecipIntensity(Double.valueOf(rootElement.getChild("after5hPrecipIntensity").getText()));
			weather.setAfter5hPrecipProbability(Double.valueOf(rootElement.getChild("after5hPrecipProbability").getText()));

			weather.setAfter7hTemperature(Double.valueOf(rootElement.getChild("after7hTemperature").getText()));
			weather.setAfter7hCloudCover(Double.valueOf(rootElement.getChild("after7hCloudCover").getText()));
			weather.setAfter7hPrecipIntensity(Double.valueOf(rootElement.getChild("after7hPrecipIntensity").getText()));
			weather.setAfter7hPrecipProbability(Double.valueOf(rootElement.getChild("after7hPrecipProbability").getText()));

			weather.setAfter8hTemperature(Double.valueOf(rootElement.getChild("after8hTemperature").getText()));
			weather.setAfter8hCloudCover(Double.valueOf(rootElement.getChild("after8hCloudCover").getText()));
			weather.setAfter8hPrecipIntensity(Double.valueOf(rootElement.getChild("after8hPrecipIntensity").getText()));
			weather.setAfter8hPrecipProbability(Double.valueOf(rootElement.getChild("after8hPrecipProbability").getText()));

			weather.setAfter9hTemperature(Double.valueOf(rootElement.getChild("after9hTemperature").getText()));
			weather.setAfter9hCloudCover(Double.valueOf(rootElement.getChild("after9hCloudCover").getText()));
			weather.setAfter9hPrecipIntensity(Double.valueOf(rootElement.getChild("after9hPrecipIntensity").getText()));
			weather.setAfter9hPrecipProbability(Double.valueOf(rootElement.getChild("after9hPrecipProbability").getText()));

			weather.setAfter10hTemperature(Double.valueOf(rootElement.getChild("after10hTemperature").getText()));
			weather.setAfter10hCloudCover(Double.valueOf(rootElement.getChild("after10hCloudCover").getText()));
			weather.setAfter10hPrecipIntensity(Double.valueOf(rootElement.getChild("after10hPrecipIntensity").getText()));
			weather.setAfter10hPrecipProbability(Double.valueOf(rootElement.getChild("after10hPrecipProbability").getText()));

			weather.setAfter11hTemperature(Double.valueOf(rootElement.getChild("after11hTemperature").getText()));
			weather.setAfter11hCloudCover(Double.valueOf(rootElement.getChild("after11hCloudCover").getText()));
			weather.setAfter11hPrecipIntensity(Double.valueOf(rootElement.getChild("after11hPrecipIntensity").getText()));
			weather.setAfter11hPrecipProbability(Double.valueOf(rootElement.getChild("after11hPrecipProbability").getText()));

			weather.setAfter13hTemperature(Double.valueOf(rootElement.getChild("after13hTemperature").getText()));
			weather.setAfter13hCloudCover(Double.valueOf(rootElement.getChild("after13hCloudCover").getText()));
			weather.setAfter13hPrecipIntensity(Double.valueOf(rootElement.getChild("after13hPrecipIntensity").getText()));
			weather.setAfter13hPrecipProbability(Double.valueOf(rootElement.getChild("after13hPrecipProbability").getText()));

			weather.setAfter14hTemperature(Double.valueOf(rootElement.getChild("after14hTemperature").getText()));
			weather.setAfter14hCloudCover(Double.valueOf(rootElement.getChild("after14hCloudCover").getText()));
			weather.setAfter14hPrecipIntensity(Double.valueOf(rootElement.getChild("after14hPrecipIntensity").getText()));
			weather.setAfter14hPrecipProbability(Double.valueOf(rootElement.getChild("after14hPrecipProbability").getText()));

			weather.setAfter15hTemperature(Double.valueOf(rootElement.getChild("after15hTemperature").getText()));
			weather.setAfter15hCloudCover(Double.valueOf(rootElement.getChild("after15hCloudCover").getText()));
			weather.setAfter15hPrecipIntensity(Double.valueOf(rootElement.getChild("after15hPrecipIntensity").getText()));
			weather.setAfter15hPrecipProbability(Double.valueOf(rootElement.getChild("after15hPrecipProbability").getText()));

			weather.setAfter16hTemperature(Double.valueOf(rootElement.getChild("after16hTemperature").getText()));
			weather.setAfter16hCloudCover(Double.valueOf(rootElement.getChild("after16hCloudCover").getText()));
			weather.setAfter16hPrecipIntensity(Double.valueOf(rootElement.getChild("after16hPrecipIntensity").getText()));
			weather.setAfter16hPrecipProbability(Double.valueOf(rootElement.getChild("after16hPrecipProbability").getText()));

			weather.setAfter17hTemperature(Double.valueOf(rootElement.getChild("after17hTemperature").getText()));
			weather.setAfter17hCloudCover(Double.valueOf(rootElement.getChild("after17hCloudCover").getText()));
			weather.setAfter17hPrecipIntensity(Double.valueOf(rootElement.getChild("after17hPrecipIntensity").getText()));
			weather.setAfter17hPrecipProbability(Double.valueOf(rootElement.getChild("after17hPrecipProbability").getText()));

			weather.setAfter18hTemperature(Double.valueOf(rootElement.getChild("after18hTemperature").getText()));
			weather.setAfter18hCloudCover(Double.valueOf(rootElement.getChild("after18hCloudCover").getText()));
			weather.setAfter18hPrecipIntensity(Double.valueOf(rootElement.getChild("after18hPrecipIntensity").getText()));
			weather.setAfter18hPrecipProbability(Double.valueOf(rootElement.getChild("after18hPrecipProbability").getText()));

			weather.setAfter19hTemperature(Double.valueOf(rootElement.getChild("after19hTemperature").getText()));
			weather.setAfter19hCloudCover(Double.valueOf(rootElement.getChild("after19hCloudCover").getText()));
			weather.setAfter19hPrecipIntensity(Double.valueOf(rootElement.getChild("after19hPrecipIntensity").getText()));
			weather.setAfter19hPrecipProbability(Double.valueOf(rootElement.getChild("after19hPrecipProbability").getText()));

			weather.setAfter20hTemperature(Double.valueOf(rootElement.getChild("after20hTemperature").getText()));
			weather.setAfter20hCloudCover(Double.valueOf(rootElement.getChild("after20hCloudCover").getText()));
			weather.setAfter20hPrecipIntensity(Double.valueOf(rootElement.getChild("after20hPrecipIntensity").getText()));
			weather.setAfter20hPrecipProbability(Double.valueOf(rootElement.getChild("after20hPrecipProbability").getText()));

			weather.setAfter21hTemperature(Double.valueOf(rootElement.getChild("after21hTemperature").getText()));
			weather.setAfter21hCloudCover(Double.valueOf(rootElement.getChild("after21hCloudCover").getText()));
			weather.setAfter21hPrecipIntensity(Double.valueOf(rootElement.getChild("after21hPrecipIntensity").getText()));
			weather.setAfter21hPrecipProbability(Double.valueOf(rootElement.getChild("after21hPrecipProbability").getText()));

			weather.setAfter22hTemperature(Double.valueOf(rootElement.getChild("after22hTemperature").getText()));
			weather.setAfter22hCloudCover(Double.valueOf(rootElement.getChild("after22hCloudCover").getText()));
			weather.setAfter22hPrecipIntensity(Double.valueOf(rootElement.getChild("after22hPrecipIntensity").getText()));
			weather.setAfter22hPrecipProbability(Double.valueOf(rootElement.getChild("after22hPrecipProbability").getText()));

			weather.setAfter23hTemperature(Double.valueOf(rootElement.getChild("after23hTemperature").getText()));
			weather.setAfter23hCloudCover(Double.valueOf(rootElement.getChild("after23hCloudCover").getText()));
			weather.setAfter23hPrecipIntensity(Double.valueOf(rootElement.getChild("after23hPrecipIntensity").getText()));
			weather.setAfter23hPrecipProbability(Double.valueOf(rootElement.getChild("after23hPrecipProbability").getText()));
			
			

		} catch (JDOMException e) {
			logger.error("Error reading xml file :", e);
		} catch (IOException e) {
			logger.error("Error openinig xml file :", e);
		}
		return weather;
	}

	public Response getLastLoadData() {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(loadRestUrl);
		WebTarget resourceWebTarget = webTarget.path(loadAppPath);
		Response responseEntity = resourceWebTarget.request(
				MediaType.APPLICATION_XML).get();
		return responseEntity;
	}

	public Response getLastWeatherData(long sampleDateParam) {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(weatherRestUrl);
		WebTarget resourceWebTarget = webTarget.path(weatherAppPath);
		WebTarget resourceWebTargetParam = resourceWebTarget.queryParam(
				"sampleDate", sampleDateParam);
		Response responseEntity = resourceWebTargetParam.request(
				MediaType.APPLICATION_XML).get();
		return responseEntity;
	}

	public CSVWriter writeLoadCsv(List<Load> listLoad) {
		String outputFile = loadCsvFile;

		try {
			// next block to comment if we desire to write in append on file
			boolean alreadyExists = new File(outputFile).exists();
			if (alreadyExists) {
				File fl = new File(outputFile);
				fl.delete();
			}

			// create a empty csv file with ";" separetor
			CsvWriter csvOutput = new CsvWriter(
					new FileWriter(outputFile, true), ';');

			Iterator<Load> it = listLoad.iterator();

			while (it.hasNext()) {
				Load histLoad = new Load();
				histLoad = it.next();

				SimpleDateFormat df = new SimpleDateFormat(
						"yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
				String dataForCsv = df
						.format(histLoad.getLoadSampleDate() * 1000);
				csvOutput.write(dataForCsv);
				csvOutput.write(String.valueOf(histLoad.getLoadSampleDate()));
				csvOutput.write(histLoad.getMeterId());

				csvOutput.write(String.valueOf(histLoad
						.getDownstreamActivePowerEEA()));

				csvOutput.write(String.valueOf(histLoad
						.getReactiveInductivePowerEEI()));

				csvOutput.write(String.valueOf(histLoad
						.getReactiveCapacitivePowerEEC()));

				csvOutput.write(String.valueOf(histLoad
						.getUpstreamActivePowerEUA()));

				csvOutput.endRecord();
			}
			csvOutput.close();
		} catch (IOException e) {
			logger.error("Error writing Load csv file :", e);
		}

		CSVWriter csvFile = null;
		return csvFile;
	}

	public CSVWriter writeWeatherCsv(WeatherForecast weather) {
		String outputFile = weatherCsvFile;

		try {
			// next block to comment if we desire to write in append on file
			boolean alreadyExists = new File(outputFile).exists();
			if (alreadyExists) {
				File fl = new File(outputFile);
				fl.delete();
			}
			// create a empty csv file with ";" separetor
			CsvWriter csvOutput = new CsvWriter(
					new FileWriter(outputFile, true), ';');
			SimpleDateFormat df = new SimpleDateFormat(
					"yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
			String dataForCsv = df.format(weather.getCurrentTime() * 1000);

			csvOutput.write(dataForCsv);
			csvOutput.write(String.valueOf(weather.getCurrentTime()));
			csvOutput.write(String.valueOf(weather.getDailySunriseTime()));
			csvOutput.write(String.valueOf(weather.getDailySunsetTime()));
			csvOutput.write(String.valueOf(weather.getTemperatureMin()));
			csvOutput.write(String.valueOf(weather.getTemperatureMax()));
			csvOutput.write(String.valueOf(weather.getCurrentTemperature()));
			csvOutput.write(String.valueOf(weather.getCurrentCloudCover()));

			csvOutput.write(String.valueOf(weather.getAfter1hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter1hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter1hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter1hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter2hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter2hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter2hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter2hPrecipProbability()));
			
			csvOutput.write(String.valueOf(weather.getAfter3hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter3hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter3hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter3hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter4hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter4hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter4hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter4hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter5hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter5hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter5hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter5hPrecipProbability()));
			
			csvOutput.write(String.valueOf(weather.getAfter6hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter6hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter6hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter6hPrecipProbability()));
			
			csvOutput.write(String.valueOf(weather.getAfter7hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter7hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter7hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter7hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter8hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter8hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter8hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter8hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter9hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter9hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter9hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter9hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter10hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter10hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter10hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter10hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter11hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter11hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter11hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter11hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter12hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter12hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter12hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter12hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter13hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter13hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter13hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter13hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter14hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter14hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter14hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter14hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter15hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter15hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter15hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter15hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter16hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter16hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter16hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter16hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter17hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter17hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter17hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter17hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter18hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter18hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter18hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter18hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter19hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter19hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter19hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter19hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter20hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter20hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter20hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter20hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter21hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter21hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter21hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter21hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter22hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter22hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter22hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter22hPrecipProbability()));

			csvOutput.write(String.valueOf(weather.getAfter23hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter23hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter23hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter23hPrecipProbability()));
			
			csvOutput.write(String.valueOf(weather.getAfter24hTemperature()));
			csvOutput.write(String.valueOf(weather.getAfter24hCloudCover()));
			csvOutput.write(String.valueOf(weather.getAfter24hPrecipIntensity()));
			csvOutput.write(String.valueOf(weather.getAfter24hPrecipProbability()));

			csvOutput.endRecord();
			csvOutput.close();
		} catch (IOException e) {
			logger.error("Error writing Weather.csv file , this is the error:",
					e);
		}
		CSVWriter csvFile = null;
		return csvFile;
	}
}
