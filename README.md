1Proyecto Fin de Máster Luis Moro Carrera

Structure

--src:  
  the source code  
    ----main.ipynb  
    ----utils.py  
    ----eDA_and_ETL.ipynb  
--datasets:  
  different steps of the data life cycle. Following the data engineering convention https://towardsdatascience.com/the-importance-of-layered-thinking-in-data-engineering-a09f685edc71   
--data visualization:  
  different figures employed for EDA and memory elaboration  



SOBRE EL ERROR

Lo que aumenta que nos da error es el Heap Size para variables de entorno etc. Es por ejemplo, si necesito un dataframer enorme que necesito llenar.

Mi error arece que tiene que ver con que el grafo de ejecucion, que ejecuta el codigo correspondiente por detrás en java (y parece que no tiene que ser aciclico), esta resolviendo algo por detrás de modo iterativo y provocando un stackoverflow. Como un dataframe es inmutable en java, no lo esta borrando al pasar de una iteracion a la siguiente. 

En cualquier caso puedes ver en hue y en spark logs lo que pasa. Pero de momento mi workaround es correcto.
http://c14-1.bd.cluster.cesga.es:39981



[11:23] Javier Cacheiro López
https://c14-18.bd.cluster.cesga.es:8090/proxy/application_1678696618277_5081/


SOBRE EL RESTO DE ETL

El momento mas caluroso ha de ser sobre las cinco de la tarde. Parece que hay un shift en nuestros datos con horario de verano y como se interpreta el timestap, porque en grafana los picos aparecen movidos. Si es necesario, mete un shift en todas las representaciones y ya.

En el caso del consumo electrico de los nodos, lo que mas influye es la propia serie temporal, no tanto las exogenas, ya que el consumo se deriva del uso intensivo que se hace del cluster por los ingenieros del CESGA. Mas jobs mas consumo.

En cuanto al consumo de las enfriadoras, el percentil 90 se deberia alcanzar con 200 kW. Nota por otra parte que si el consumo es cero es muy raro porque siempre hay un consumo base minimo de la bomba (cada enfriadora tiene una bomba y otra de recambio por si se estropea falla etc), y eso ya te hace 10kW bombeando agua. Esto lleva a pensar que los ceros son en reinicios. (Los dos a menos de 10kW, parados=

La presion del compresor en reposo es de 7.5 bares y es la del chiller.

Puede pasar que si solo nos quedamos con la presion maxima conjunta de los compresores, nos encontremos por ejemplo 20 bares. Pero no es lo mismo 20 bares de un compresor y los demas en reposo de 7.5 los de enfriadora 1 y 5 los de 2 a que todos esten a tope y todos en torno a 20 bares etc. Necesitamos una feature para dar cuenta de esto a nuestro modelo, y es la del numero de compresores trabajando. Para entrenar supon que en el instante siguiente si sabes la presion de los compresores, en el instante siguiente, luego en inferencia ya no y tendrias que hacer forecast, claro.  

LA MAQUINA
Primero tienes bateria de refrigeracion que es parecida a la de un coche, es por la que pasa el agua por el cpd, es la climatizadora como tal. Este agua fria proveniente de nuestra refrigeradora en la iteraccion previa del ciclo de refrigeracion absorbe el calor del aire caliente del CPD y se calienta a 18-25 grados, que es como llega en el flujo de caudal a la enfriadora.

Cadas enfriadora tiene una bomba que hace fluir el agua (y te hace el consumo minimo mencionado) mas una de repuesto por msi no funciona. Si no funciolna la bomba el agua se estanca o se cierra la compuerta y solo pasa a circular por la otra. El t In es el que sale de la climatizadora y llega a esta maquina.
El T evap es la temperatura despues de pasar por el segundo elemento de nuestra enfruiadora, que es el evaporador. Funciona de modo similar al condensador de uina nevera. Circula por una bateria. 
Si la T evap es menor que t In es que se ha enfriado el agua sin necesirtar de usar los compresores, que son los que enfrian activamente el agua en caso de que el free cooling sea insuficiente.

De modo que T in y T ambiente deben ser las mismas en ambas enfriadoras
T out y T evap pueden ser distintas en caso de que una funcione y otra no.

Para las temperaturas toma las medias cada 30 min y luego …


EN CUANTO A TEMPERATURAS

Pûedo tener valor distintos en t evaporador y t out
T in y T ambiente deberia ser la misma en ambas enfriadoras (el afgua que vuiene del cpd de refrigerar su aire es el Tin, y el T ambiente es despues de salir del compresor
Temperatura. En principio se toma la de la enfriadora activa. Cuando no funciona ninguna solo en resets y se puede tomar cualquiera de las dos. Cuando funcionan ambas t in y t ambiente deben ser las mismas, coge por ejemplo la primera, y para 

SERIES A FORECASTEAR

Como variable exogena queremos tambien el numero de nodos funcionando en cada vetana de 30 minutos basandonos en el Thrshold del consumo

Presion 2 veranos, que es cuando no funciona el free cooling y no queda otra quw refrigerar con el compresor.
Consumos dependen principalmente del uso que se haga de la refrigeradora, asi que en principio dira que dos veranos concatenados, si no tienen outlayer de primeras mejor


https://drive.google.com/drive/folders/16S9auqEF8tEbYmk33h50n3uHy9U5SoD-
![image](https://github.com/lmc00/TFM/assets/56820165/6127500f-d4a1-40fa-be0f-4b58bbd594cd)
