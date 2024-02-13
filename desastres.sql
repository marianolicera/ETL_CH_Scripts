---Microdesafío Semana 4---

---CARGA DE DATOS---

---Creamos la Base de Datos---
CREATE DATABASE DESASTRES;
GO

---Creamos la tabla Clima---
CREATE TABLE Clima
(idClima INT PRIMARY KEY IDENTITY NOT NULL,
año INT NOT NULL,
Temperatura FLOAT NOT NULL,
Oxigeno FLOAT NOT NULL);
GO

---Insertamos valores---
INSERT INTO Clima (año, Temperatura, Oxigeno) VALUES (2023, 22.5,230);
INSERT INTO Clima (año, Temperatura, Oxigeno) VALUES (2024, 22.7,228.6);
INSERT INTO Clima (año, Temperatura, Oxigeno) VALUES (2025, 22.9,227.5);
INSERT INTO Clima (año, Temperatura, Oxigeno) VALUES (2026, 23.1,226.7);
INSERT INTO Clima (año, Temperatura, Oxigeno) VALUES (2027, 23.2,226.4);
INSERT INTO Clima (año, Temperatura, Oxigeno) VALUES (2028, 23.4,226.2);
INSERT INTO Clima (año, Temperatura, Oxigeno) VALUES (2029, 23.6,226.1);
INSERT INTO Clima (año, Temperatura, Oxigeno) VALUES (2030, 23.8,225.1);

---Creamos la tabla Desastres---
CREATE TABLE Desastres
(idDesastre INT PRIMARY KEY IDENTITY NOT NULL,
año INT NOT NULL,
Tsunamis INT NOT NULL,
Olas_Calor INT NOT NULL,
Terremotos INT NOT NULL,
Erupciones INT NOT NULL,
Incendios INT NOT NULL
);
GO

---Insertamos valores en la tabla Desastres---
INSERT INTO Desastres (año,Tsunamis,Olas_Calor,Terremotos,Erupciones,Incendios) VALUES (2023, 2,15, 6,7,50);
INSERT INTO Desastres (año,Tsunamis,Olas_Calor,Terremotos,Erupciones,Incendios) VALUES (2024, 1,12, 8,9,46);
INSERT INTO Desastres (año,Tsunamis,Olas_Calor,Terremotos,Erupciones,Incendios) VALUES (2025, 3,16, 5,6,47);
INSERT INTO Desastres (año,Tsunamis,Olas_Calor,Terremotos,Erupciones,Incendios) VALUES (2026, 4,12, 10,13,52);
INSERT INTO Desastres (año,Tsunamis,Olas_Calor,Terremotos,Erupciones,Incendios) VALUES (2027, 5,12, 6,5,41);
INSERT INTO Desastres (año,Tsunamis,Olas_Calor,Terremotos,Erupciones,Incendios) VALUES (2028, 4,18, 3,2,39);
INSERT INTO Desastres (año,Tsunamis,Olas_Calor,Terremotos,Erupciones,Incendios) VALUES (2029, 2,19, 5,6,49);
INSERT INTO Desastres (año,Tsunamis,Olas_Calor,Terremotos,Erupciones,Incendios) VALUES (2030, 4,20, 6,7,50);

---Creamos la tabla Muertes---
CREATE TABLE Muertes
(idMuertes INT PRIMARY KEY IDENTITY NOT NULL,
año INT NOT NULL,
R_Menor15 INT NOT NULL,
R_15_a_30 INT NOT NULL,
R_30_a_45 INT NOT NULL,
R_45_a_60 INT NOT NULL,
R_M_a_60 INT NOT NULL
);
GO

---Insertamos valores en la tabla Muertes---
INSERT INTO Muertes (año,R_Menor15,R_15_a_30,R_30_a_45,R_45_a_60,R_M_a_60) VALUES (2023, 1000,1300, 1200,1150,1500);
INSERT INTO Muertes (año,R_Menor15,R_15_a_30,R_30_a_45,R_45_a_60,R_M_a_60) VALUES (2024, 1200,1250, 1260,1678,1940);
INSERT INTO Muertes (año,R_Menor15,R_15_a_30,R_30_a_45,R_45_a_60,R_M_a_60) VALUES (2025, 987,1130, 1160,1245,1200);
INSERT INTO Muertes (año,R_Menor15,R_15_a_30,R_30_a_45,R_45_a_60,R_M_a_60) VALUES (2026, 1560,1578, 1856,1988,1245);
INSERT INTO Muertes (año,R_Menor15,R_15_a_30,R_30_a_45,R_45_a_60,R_M_a_60) VALUES (2027, 1002,943, 1345,1232,986);
INSERT INTO Muertes (año,R_Menor15,R_15_a_30,R_30_a_45,R_45_a_60,R_M_a_60) VALUES (2028, 957,987, 1856,1567,1756);
INSERT INTO Muertes (año,R_Menor15,R_15_a_30,R_30_a_45,R_45_a_60,R_M_a_60) VALUES (2029, 1285,1376, 1465,1432,1236);
INSERT INTO Muertes (año,R_Menor15,R_15_a_30,R_30_a_45,R_45_a_60,R_M_a_60) VALUES (2030, 1145,1456, 1345,1654,1877);

---Creación de la BD OLAP donde se almacenaran las estadísticas---
CREATE DATABASE DESASTRES_BDE;
GO

USE DESASTRES_BDE;
GO

CREATE TABLE DESASTRE_FINAL(
Cuatrenio VARCHAR(20) NOT NULL PRIMARY KEY,
Temperatura_Promedio FLOAT NOT NULL,
Oxigeno_Promedio FLOAT NOT NULL,
T_Terremto INT NOT NULL,
T_Tsunami INT NOT NULL,
T_OlasCalor INT NOT NULL,
T_Erupciones INT NOT NULL,
T_Incendios INT NOT NULL,
M_Menores_15_Promedio FLOAT NOT NULL,
M_De_15_A_30_Promedio FLOAT NOT NULL,
M_De_30_A_45_Promedio FLOAT NOT NULL,
M_De_45_A_60_Promedio FLOAT NOT NULL,
M_Mayores_60_Promedio FLOAT NOT NULL,
);
GO

---TRANSFORMACION DE DATOS---
---Creamos procedimiento ETL---
USE DESASTRES;
GO

CREATE PROCEDURE pETL_Desastres
AS
DELETE FROM DESASTRES_BDE.dbo.DESASTRE_FINAL;
INSERT INTO DESASTRES_BDE.dbo.DESASTRE_FINAL
SELECT x.Cuatrenio, AVG(x.Temperatura) AS Temperatura_Promedio, AVG(x.NivelOxigeno) AS Oxigeno_Promedio,
SUM(x.Terremotos) AS T_Terremoto, SUM(x.Tsunamis) AS T_Tsunami, SUM(x.Olas_Calor) AS T_OlasCalor, SUM(x.Erupciones) AS T_Erupciones, SUM(x.Incendios) AS T_Incendios,
AVG(x.Menores15) AS M_Menores_15_Promedio, AVG(x.De15A30) AS M_De_15_A_30_Promedio, AVG(x.De30A45) AS M_De_30_A_45_Promedio, AVG(x.De45A60) AS M_De_45_A_60_Promedio, AVG(x.Mayores60) AS M_Mayores_60_Promedio
FROM
(SELECT CASE WHEN c.año < 2027 THEN '2023-2026' ELSE '2027-2030' END AS Cuatrenio,
Temperatura = c.Temperatura,
NivelOxigeno = c.Oxigeno,
Terremotos = d.Terremotos,
Tsunamis = d.Tsunamis,
Olas_Calor = d.Olas_Calor,
Erupciones = d.Erupciones,
Incendios = d.Incendios,
Menores15 = m.R_Menor15,
De15A30 = m.R_15_a_30,
De30A45 = m.R_30_a_45,
De45A60 = m.R_45_a_60,
Mayores60 = m.R_M_a_60
FROM DESASTRES.dbo.Clima as c
JOIN DESASTRES.dbo.Desastres as d ON c.año = d.año
JOIN DESASTRES.dbo.Muertes as m ON d.año = m.año) x
GROUP BY x.Cuatrenio


---Ejecutamos el procedimiento---
EXECUTE pETL_Desastres;
GO

---Verificamos---
USE DESASTRES_BDE;
GO

SELECT * FROM dbo.DESASTRE_FINAL;
GO