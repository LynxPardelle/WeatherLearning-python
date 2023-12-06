/* 
  Crear tablas que se guardarán en la base de datos de 
  redshift con la información de archivo example_json_table.json
 */
CREATE SCHEMA IF NOT EXISTS weather;
USE = weather;
CREATE TABLE foreCastData (
  foreCastDataId INT IDENTITY(1,1) PRIMARY KEY NOT NULL UNIQUE,
  Date DATE,
  MinimumTemperature VARCHAR(10) ,
  MaximumTemperature VARCHAR(10),
  DayPhrase VARCHAR(100),
  DayHasPrecipitation BOOLEAN,
  NightPhrase VARCHAR(100),
  NightHasPrecipitation BOOLEAN,
);
CREATE TABLE timeZone (
  timeZoneId INT IDENTITY(1,1) PRIMARY KEY NOT NULL UNIQUE,
  Code VARCHAR(10),
  Name VARCHAR(100),
  GmtOffset INT,
  IsDaylightSaving BOOLEAN,
);
CREATE TABLE administrativeArea (
  administrativeAreaId INT IDENTITY(1,1) PRIMARY KEY NOT NULL UNIQUE,
  ID VARCHAR(10),
  LocalizedName VARCHAR(100),
  EnglishName VARCHAR(100),
  Level INT,
  LocalizedType VARCHAR(100),
  EnglishType VARCHAR(100),
  CountryID VARCHAR(10),
);
CREATE TABLE country (
  countryId INT IDENTITY(1,1) PRIMARY KEY NOT NULL UNIQUE,
  ID VARCHAR(10),
  LocalizedName VARCHAR(100),
  EnglishName VARCHAR(100),
);
CREATE TABLE region (
  regionId INT IDENTITY(1,1) PRIMARY KEY NOT NULL UNIQUE,
  ID VARCHAR(10),
  LocalizedName VARCHAR(100),
  EnglishName VARCHAR(100),
);
CREATE TABLE localization (
  localizationId INT IDENTITY(1,1) PRIMARY KEY NOT NULL UNIQUE,
  ID VARCHAR(10),
  LocalizedName VARCHAR(100),
  EnglishName VARCHAR(100),
  PrimaryPostalCode VARCHAR(10),
  RegionId INT,
  FOREIGN KEY (RegionId) REFERENCES region(regionId),
  CountryId VARCHAR(10),
  FOREIGN KEY (CountryId) REFERENCES country(countryId),
  AdministrativeAreaId INT,
  FOREIGN KEY (AdministrativeAreaId) REFERENCES administrativeArea(administrativeAreaId),
  TimeZoneId INT,
  FOREIGN KEY (TimeZoneId) REFERENCES timeZone(timeZoneId),
  Latitude VARCHAR(100),
  Longitude VARCHAR(100),
  Elevation INT,
);
CREATE TABLE foreCastAndLocalization (
  foreCastAndLocalizationId INT IDENTITY(1,1) PRIMARY KEY NOT NULL UNIQUE,
  foreCastDataId INT,
  FOREIGN KEY (foreCastDataId) REFERENCES foreCastData(foreCastDataId),
  localizationId INT,
  FOREIGN KEY (localizationId) REFERENCES localization(localizationId),
  date DATE,
);