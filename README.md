Estos son unos scripts que desarrolle en el 2015 para un concurso de Santarder donde nos daban 2 a~nos y medio
de informacion para predecir el comportamiento en los proximos 3 meses y ver si son o no clientes activos. 

Mi estrategia era la siguiente:

* Agarrar una muestra de 2-3 mil personas empezando en un mes tal que en 6 meses sea igual al mes final que se pretende predecir. En otras palabras si se pretendia predecir hasta Junio del 2016, Agarraba una muestra de Junio del 2015 para que en 6 meses sea Junio del 2015.

* De esa muestra buscaba su informacion en los proximos 6 meses y los separaba por antiguedad. 

* Elegi un algoritmo que usaba la fecha y otras variables constantes o predecibles (en este caso seria edad o sexo) como kernel base y los valores importantes que vamos a predecir. 

* Entrenaba el algoritmo con mi muestra ya obtenida. 

* Obtenia otra muestra pero en vez de predecir valores desconocidos pretendia probar el algoritmo con valores conocidos. Agarraba otra muestra de 2 mil personas que conociera 6 meses de su actividad y alimentaba al algoritmo con los primeros 3 meses y queria ver que tan preciso era el algoritmo.

* Vi que era no muy bueno prediciendo y teniamos el tiempo encima asi que no logre llegar muy lejos.

Para mas informacion o quieran preguntarme sobre este projecto mandar un correo a: noturnbak@hotmail.com o hhawley@snolab.ca