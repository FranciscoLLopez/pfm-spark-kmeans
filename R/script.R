#Dataset for Clustering. PFM Francisco Lopez. 
install.packages("cluster")
install.packages("clue")
install.packages("fpc")
install.packages("NbClust")

library(cluster)
library(clue)
library(fpc)
library(NbClust)

# Carga fichero
setwd("~/dataset/creditcardfraud")
datos.csv <- read.csv("creditcard.csv",header=T)

# Se comprueba si se ha cargado correctamente el fichero
head(datos.csv)

# Eliminamos datos que no propocionan valor: primera columna y las dos ultimas.
datos_numericos <- datos.csv[,2:29]
head(datos_numericos)

#Paso 1. Normalizacion de variables.
# Examinar variabilidad de las variables numericas
sapply(datos_numericos, var)

# Se realiza el conjunto normalizado, 
df_normalizadas = as.data.frame(scale(datos_numericos),center=TRUE,scale=TRUE)

#Comprobacion de datos normalizados
head(df_normalizadas)
summary(df_normalizadas)

sapply(df_normalizadas, sd)  # obviamente la desviaciÃ³n es 1 para todas
sapply(df_normalizadas, mean) # y la media es practicamente cero para todas

#PCA con variables normalizadas
df_total_stand <- prcomp(df_normalizadas, center=FALSE, scale=FALSE)
capture.output(df_total_stand, file = "df_total_stand.txt") 
# Conocer la varianza que recoge cada componente
plot(df_total_stand, main = "Varianza de cada componente")
# Grrafico para ver el mostrar el codo
screeplot(df_total_stand,
          main = "Varianza de cada componente",
          type="lines",
          col=3)

# ProporciÃ³n de varianza del total y proporciÃ³n acumulada
summary(df_total_stand)


mydata<-df_normalizadas[1:5000,]
asw <- numeric(20)
for (k in 2:20)
  asw[[k]] <- pam(mydata, k) $ silinfo $ avg.width
k.best <- which.max(asw)
d <- dist(mydata, method = "euclidean") #
cat("número de grupos estimados por la media de silhouette", k.best, "\n")
plot(pam(d, k.best))


# K-Means Cluster Analysis
fit <- kmeans(mydata,2)
fit$cluster
fit$centers

# get cluster means 
aggregate(mydata,by=list(fit$cluster),FUN=mean)
# append cluster assignment
mydata <- data.frame(mydata, clusterid=fit$cluster)
plot(mydata$x,mydata$y, col = fit$cluster, main="K-means Clustering results")

clus <- kmeans(mydata, centers=k.best)
clusplot(mydata, clus$cluster, color=TRUE, shade=TRUE,labels=k.best,lines=0)


plotcluster(mydata, clus$cluster)

nc <- NbClust(mydata,
              min.nc=2, max.nc=20,
              method="kmeans")
barplot(table(nc$Best.n[1,]),
        xlab="Numer of Clusters",
        ylab="Number of Criteria",
        main="Number of Clusters Chosen")





