# pacotes para leitura e manipulação 

# install.packages("data.table")
# install.packages("dplyr")
# install.packages("readxl")
# install.packages("textreadr")

library(data.table)
library(dplyr)
library(readxl)
library(textreadr)
library(bit64)


rm(list = ls())
gc()


# Leitura das variáveis da base empregados ----

# Definição de layout e exploração iniciação

#> RAIS 1999 a 2008 empregados ----
caminho <- getwd()
ano <- 2008
renamesdicio <- read_excel(file.path(caminho,"1files","layout_vinc_1999-2020_harmonizacao_variaveis.xlsx"))
nlimteste <- 10^3
base_full <- NULL
for(ano in 1999:2008){
  print(ano)
  arquivos <- paste0("\\\\STORAGE6\\bases\\DADOS\\RESTRITO\\RAIS\\RAIS_Bruta\\RAIS_OP_",ano)
  lista_arquivos<-list.files(arquivos,full.names = T,include.dirs = T,recursive = T)
  tamfile <- file.size(lista_arquivos)
  lista_arquivos <- lista_arquivos[tamfile>(10^7)]
  lista_arquivos <- lista_arquivos[!grepl("estb",lista_arquivos,ignore.case = T)]
  
  base <- rbindlist(lapply(lista_arquivos,function(x) fread(file=x,nrows =  nlimteste,
                                                            skip = 0,sep=";",header = T,dec = ",")))
  varschange <- renamesdicio$nome_var_padrao[renamesdicio$nome_var_rais_bruta %in% names(base)]
  setnames(base,names(base)[!is.na(varschange)],varschange[!is.na(varschange)])
  base[,ano:=ano]
  # saveRDS(base,file.path(caminho,"5temp",paste0("temp_",ano,".rds")))
  
  
  
  base_full <- rbind(base_full,base,fill=T)
}



##> Arquivos de 2009 a 2020 da RAIS empregados ==----
mapa <- read_excel(file.path(caminho,"1files","Resumo_arquivos.xlsx"))
ano <- 2019
base_full_2 <- NULL
for(ano in 2009:2020){
  print(ano)
  pasta_empr <- mapa$PASTA_EMPR[mapa$RAIS==ano]
  lista_arquivos<-list.files(pasta_empr,full.names = T,recursive = F)
  tamfile <- file.size(lista_arquivos)
  lista_arquivos <- lista_arquivos[tamfile>(10^7)]
  tem_layout <- mapa$LAY_EMPR[mapa$RAIS==ano]
  # padlay <- read_excel(path=tem_layout)
  edoc <- grepl("doc$",tem_layout)
  if(edoc){
    padlay <- read_doc(file=tem_layout)  
    
    if(ano<2017){
      padlay <- padlay[nchar(padlay)>70]
      
      padlay <- as.data.frame(do.call(rbind,strsplit(padlay," +")))
      padlay <- padlay %>% mutate(V2 = as.numeric(V2)) %>%
        filter(!is.na(V2))
      # última variávael tamanho 1
      vecs <- c(diff(padlay$V2),1)
      nvecs <- gsub("-","_",padlay$V4,fixed = T)
      typvec <-  gsub("-","_",padlay$V5,fixed = T)
      
    }else{
      
      padlay <- padlay[nchar(padlay)==75]
      
      vecs <- as.vector(na.omit(as.numeric(substr(padlay,60,65))))
      nvecs <-  gsub(" ","",substr(padlay,15,50)[-1],fixed = T)
      nvecs <-  gsub("-","_",nvecs,fixed = T)
      typvec <-  gsub(" ","",substr(padlay,51,60)[-1],fixed = T)
      

      
    }
  }else{
    padlay <- read_excel(tem_layout,skip = 5,col_names =FALSE)
    names(padlay) <- c("V1","V2","V3","V4","V5")
    padlay <- padlay %>% mutate(V4 = as.numeric(V4)) %>%
      filter(!is.na(V4))
    vecs <- padlay$V4
    nvecs <- gsub("-","_",padlay$V2,fixed = T)
    typvec <-  gsub("-","_",padlay$V3,fixed = T)
  }

  layoutano <- data.frame(tamanho_caracters = vecs,
                          nomes_variaveis = nvecs,
                          tipo_variavel = typvec,
                          ano = ano) 
  
saveRDS(layoutano,file.path(caminho,"1files",paste0("layout_",ano,".rds")))
}
