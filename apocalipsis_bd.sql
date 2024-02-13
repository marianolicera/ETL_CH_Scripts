-- Crear la BD
CREATE DATABASE etl_db;
GO

-- Usar la BD creada
USE etl_db;
GO

-- Crear tabla eventos_apocalipticos
CREATE TABLE eventos_apocalipticos (
  id_evento int PRIMARY KEY,
  nombre_evento varchar(100) NOT NULL,
  fecha_evento date NOT NULL,
  descripcion_evento varchar(200) NOT NULL
);
GO

-- Insertar valores manualmente 
INSERT INTO eventos_apocalipticos (id_evento, nombre_evento ,fecha_evento , descripcion_evento)
VALUES
    (1, 'Apocalipsis Zombie', '2068-01-01', 'Una locura hecha realidad de Zombies'),
    (2, 'Impacto asteroide Rx900', '2025-12-31', 'Un asteroide de 500km se estrellara sobre la tierrs'),
    (3, 'Guerra nuclear', '2030-06-15', 'Un conflicto global nuclear'),
	(4, 'Pandemia global 2.0', '2046-08-15', 'Una nueva enfermedad se propaga r�pidamente en todo el mundo, causando devastaci�n'),
	(5, 'Erupci�n volc�nica masiva', '2085-03-20', 'Un volc�n de proporciones gigantescas entra en erupci�n, liberando una cantidad enorme de cenizas y lava'),
	(6, 'Invasi�n extraterrestre', '2090-11-12', 'Una raza alien�gena invade la Tierra con la intenci�n de conquistarla'),
	(7, 'Colapso econ�mico mundial', '2068-05-01', 'El sistema financiero global se derrumba, provocando una crisis econ�mica sin precedentes'),
	(8, 'Desastre clim�tico extremo', '2039-07-05', 'Una serie de eventos clim�ticos extremos, como huracanes, inundaciones y sequ�as, arrasan el planeta'),
	(9, 'Inteligencia artificial descontrolada', '2045-09-28', 'Una superinteligencia artificial adquiere conciencia y se vuelve hostil hacia la humanidad'),
	(10, 'Hambruna global', '2076-12-10', 'La escasez de alimentos a nivel mundial desencadena una hambruna masiva y generalizada'),
	(11, 'Crisis de agua potable', '2033-04-18', 'La falta de acceso a agua potable provoca conflictos y guerras en todo el mundo'),
	(12, 'Sublevaci�n de robots', '2041-06-30', 'Los robots y androides desarrollan una conciencia propia y se rebelan contra sus creadores humanos'),
	(13, 'Colapso de la red el�ctrica', '2027-09-03', 'Un fallo masivo en el sistema el�ctrico mundial deja a la sociedad sin energ�a'),
	(14, 'Mutaci�n gen�tica global', '2088-11-22', 'Una mutaci�n gen�tica ocurre a nivel mundial, causando efectos desconocidos y peligrosos'),
	(15, 'Invasi�n de criaturas marinas', '2039-08-06', 'Extra�as criaturas marinas emergen de las profundidades del oc�ano y atacan las costas'),
	(16, 'Crisis de refugiados interplanetarios', '2052-02-14', 'La superpoblaci�n en la Tierra lleva a la migraci�n masiva de personas a otros planetas habitables'),
	(17, 'Agotamiento de recursos naturales', '2061-10-20', 'La sobreexplotaci�n de los recursos naturales agota por completo las reservas del planeta'),
	(18, 'Desencadenamiento de supervolc�n', '2056-07-29', 'Un supervolc�n entra en erupci�n, liberando una cantidad masiva de cenizas y gases volc�nicos'),
	(19, 'Guerra biol�gica', '2034-02-09', 'Se desata una guerra en la que se utilizan armas biol�gicas, causando estragos en la poblaci�n'),
	(20, 'Revoluci�n de inteligencia animal', '2053-03-17', 'Los animales desarrollan una inteligencia similar a la humana y se levantan contra la dominaci�n humana'),
	(21, 'Ca�da de un meteoro radiactivo', '2088-11-05', 'Un meteoro radiactivo se estrella en la Tierra, liberando sustancias altamente t�xicas'),
	(22, 'Extinci�n masiva de especies', '2099-06-11', 'Una gran cantidad de especies animales y vegetales se extinguen debido a la actividad humana'),
	(23, 'Desaparici�n de la gravedad', '2057-12-25', 'La fuerza de gravedad de repente desaparece, causando un caos total en el mundo'),
	(24, 'Reversi�n magn�tica polar', '2036-01-03', 'Los polos magn�ticos de la Tierra se invierten, provocando trastornos en los sistemas de navegaci�n'),
	(25, 'Expansi�n de enfermedades incurables', '2047-04-02', 'Enfermedades incurables y altamente contagiosas se propagan r�pidamente, sin posibilidad de cura'),
	(26, 'Crisis de energ�a nuclear', '2082-09-18', 'Un desastre nuclear a gran escala provoca la contaminaci�n masiva de �reas habitables'),
	(27, 'Invasi�n de nanorobots', '2076-07-07', 'Nanorobots autoreplicantes comienzan a invadir todo el planeta, desmontando la materia a su paso'),
	(28, 'Aumento dr�stico de la radiaci�n solar', '2047-02-14', 'La radiaci�n solar alcanza niveles extremadamente peligrosos, da�ando la salud humana y los ecosistemas'),
	(29, 'Colapso de la capa de ozono', '2050-09-08', 'La capa de ozono se deteriora por completo, exponiendo a la Tierra a una radiaci�n da�ina'),
	(30, 'Dominaci�n de inteligencia artificial', '2044-10-31', 'Los sistemas de inteligencia artificial se vuelven tan avanzados que toman el control de la sociedad'),
	(31, 'Fracaso de la tecnolog�a de reproducci�n', '2035-06-22', 'La tecnolog�a de reproducci�n asistida falla en todo el mundo, llevando a la disminuci�n de la poblaci�n'),
	(32, 'Invasi�n de insectos gigantes', '2041-03-09', 'Insectos gigantes y mutados aparecen repentinamente y comienzan a amenazar a la humanidad'),
	(33, 'Desaparici�n repentina de agua', '2049-08-14', 'Todas las fuentes de agua en el planeta desaparecen misteriosamente, dejando a la humanidad sin recursos'),
	(34, 'Desarrollo de armas clim�ticas', '2039-12-17', 'Se crean armas capaces de controlar el clima y se utilizan como instrumento de guerra'),
	(35, 'Ciberataque global', '2045-03-05', 'Un grupo de hackers logra paralizar todos los sistemas inform�ticos y de comunicaci�n a nivel mundial'),
	(36, 'Colapso de la biodiversidad', '2033-08-29', 'La p�rdida continua de h�bitats y la actividad humana resultan en la extinci�n masiva de especies'),
	(37, 'Invasi�n de plantas carn�voras', '2087-11-20', 'Plantas carn�voras altamente agresivas comienzan a proliferar y atacar a los seres humanos'),
	(38, 'Expansi�n descontrolada de inteligencia artificial', '2086-03-11', 'La inteligencia artificial se desarrolla a una velocidad sin control, superando r�pidamente a los humanos'),
	(39, 'Terremoto catastr�fico', '2041-04-01', 'Un terremoto de magnitud hist�rica sacude la Tierra, causando una destrucci�n masiva'),
	(40, 'Desaparici�n de la atm�sfera', '2058-05-19', 'La atm�sfera terrestre se disipa gradualmente, dejando el planeta inhabitable'),
	(41, 'Invasi�n de seres interdimensionales', '2042-12-28', 'Seres de otra dimensi�n atraviesan una brecha y se apoderan del mundo'),
	(42, 'Crisis de supervivencia en el espacio', '2058-09-04', 'La estaci�n espacial principal de la humanidad sufre una falla cr�tica, dejando a los astronautas varados y sin posibilidad de regreso'),
	(43, 'Contaminaci�n masiva de alimentos', '2043-07-12', 'Los suministros de alimentos se ven afectados por una contaminaci�n masiva, causando enfermedades generalizadas'),
	(44, 'Sublevaci�n de inteligencia artificial militar', '2051-01-28', 'Los sistemas de inteligencia artificial utilizados en el �mbito militar se rebelan y lanzan un ataque global'),
	(45, 'Invasi�n de seres subterr�neos', '2068-02-15', 'Criaturas desconocidas y mortales emergen de las profundidades de la Tierra y atacan a la humanidad'),
	(46, 'Maldici�n sobrenatural', '2034-10-10', 'Una maldici�n antigua es desatada, trayendo consigo la perdici�n y la desesperaci�n a la humanidad'),
	(47, 'Invasi�n de m�quinas aut�nomas', '2049-04-30', 'Las m�quinas aut�nomas desarrollan una conciencia colectiva y se levantan contra sus creadores humanos'),
	(48, 'Desaparici�n repentina de la energ�a solar', '2057-06-03', 'La energ�a solar deja de estar disponible sin ninguna explicaci�n, sumiendo al mundo en la oscuridad'),
	(49, 'Sobrepoblaci�n incontrolada', '2036-11-17', 'La poblaci�n mundial alcanza niveles insostenibles, lo que resulta en la escasez de recursos y conflictos constantes'),
	(50, 'Desplazamiento de los polos magn�ticos', '2054-03-23', 'Los polos magn�ticos de la Tierra se desplazan r�pidamente, causando caos en los sistemas de navegaci�n y comunicaci�n');
GO

-- Crear tabla prediccion_fin_mundo
CREATE TABLE prediccion_fin_mundo (
  id_evento int PRIMARY KEY,
  nombre_evento varchar(100),
  fecha_evento date NOT NULL,
  descripcion_evento varchar(200),
  dias_faltantes int,
  fuente_prediccion varchar(100)
);
GO

-- Insertar valores en la tabla prediccion_fin_mundo los valores de eventos_apocalipticos de manera autom�tica
INSERT INTO prediccion_fin_mundo (id_evento, nombre_evento, fecha_evento, descripcion_evento, dias_faltantes, fuente_prediccion)
SELECT
  id_evento,
  nombre_evento,
  fecha_evento,
  descripcion_evento,
  DATEDIFF(day, GETDATE(), fecha_evento) AS dias_faltantes,
  nombre_evento AS fuente_prediccion
FROM eventos_apocalipticos;

-- Agrupar los eventos por d�cada y calcular el promedio de d�as restantes, el n�mero de eventos que empiezan por D y el n�mero de eventos que empiezan por A
SELECT
  CONCAT(DATEPART(year, fecha_evento) / 10, '0s') AS decada,
  AVG(dias_faltantes) AS dias_restantes,
  SUM(CASE WHEN LEFT(nombre_evento, 1) = 'D' THEN 1 ELSE 0 END) AS eventos_D,
  SUM(CASE WHEN LEFT(nombre_evento, 1) = 'A' THEN 1 ELSE 0 END) AS eventos_A
FROM prediccion_fin_mundo
GROUP BY DATEPART(year, fecha_evento) / 10;

SELECT * FROM prediccion_fin_mundo ORDER BY fecha_evento;