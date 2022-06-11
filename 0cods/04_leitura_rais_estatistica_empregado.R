gc()
rm(list = ls())
library(readxl)
library(data.table)
library(dplyr)
library(tidyverse)

caminho <- "\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA"
lista_arquivos<-list.files(pattern = "BASE_RAIS_BRUTA_UNICA",caminho,full.names = T,recursive = F)
i <- 2019
for(i in 1:8){
  print(i)
  base <- readRDS(lista_arquivos[i])
  ano <- base$ano[1]
  res<-base[,.N,by=c("tipo_duplicata","ano")]
  saveRDS(res,
          paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\NROW_BRUTA_",ano,".rds"))
  rm(base);gc()
}

caminho2 <- "\\\\storage6\\bases\\DADOS\\RESTRITO\\RAIS\\csv"
lista_arquivos<-list.files(pattern = "brasil.*",caminho2,full.names = T,recursive = F)
anos <- as.numeric(gsub(".*brasil(\\d{4}).csv","\\1",lista_arquivos))
lista_arquivos<-lista_arquivos[anos>2010]
anos<-anos[anos>2010]

i <- 1
for(i in 1:length(anos)){
  print(i)
  base <- fread(lista_arquivos[i],nrows = -1)
  ano <- anos[i]
  names(base)
  res<-data.frame(base[,.N])
  res$ano <- ano
  saveRDS(res,
          paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\NROW_ESTAT_",ano,".rds"))
  rm(base);gc()
}


caminho <- "\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA"
lista_arquivos<-list.files(pattern = "NROW.*",caminho,full.names = T,recursive = F)
base <- NULL
i <- 12
for(i in 1:length(lista_arquivos)){
  
  base_temp <- readRDS(lista_arquivos[i])
  tipo_base <- gsub(".*(ESTAT|BRUTA).*","\\1",lista_arquivos[i])
  if(tipo_base=="ESTAT"){
    names(base_temp)[1] <- "N"
  }
  base_temp$tipo_base <- tipo_base
  base <- rbindlist(list(base,base_temp),fill = T)
  print(i)
}

base[,.(Ntotal = sum(N,na.rm = T)),by=c("tipo_base","ano")]


res1 <- base %>%
  group_by(tipo_base,ano) %>%
  summarise(Ntotal = sum(N,na.rm=T)) %>%
  spread(key="tipo_base",value = "Ntotal")

write.csv2(res1,file = file.path("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA","N_BRUTA_EST.csv"),row.names = F)


res2 <- base[,prop_fonte:=(N/sum(N,na.rm = T))*100,by=c("tipo_base","ano")] %>%
  mutate(prop_fonte=prop_fonte) %>%
  filter(tipo_base=="BRUTA") %>%
  select(prop_fonte,ano,tipo_duplicata) %>%
  spread(key="tipo_duplicata",value = "prop_fonte")

write.csv2(res2,
           file = file.path("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA",
                            "N_BRUTA_FONTE.csv"),
           row.names = F)



func_vl_rais_est <- function(x){
  x <- gsub(".","",x,fixed = T)
  x <- gsub(",",".",x,fixed = T)
  x <- as.numeric(x)
  return(x)
}


### duplicatas estatística

caminho2 <- "\\\\storage6\\bases\\DADOS\\RESTRITO\\RAIS\\originais"
lista_arquivos<-list.files(pattern = ".*RAIS_VINC.*txt",caminho2,
                           full.names = T,recursive = T)
anos <- as.numeric(gsub(".*RAIS_VINC.(\\d{4})(.*).txt","\\1",lista_arquivos))
lista_arquivos<-lista_arquivos[anos>2009]
anos<-anos[anos>2009]
uniqueanos <- unique(anos)
i <- 9
for(i in 7:1){
  print(i)
  ano <- uniqueanos[i]
  base <- rbindlist(lapply(lista_arquivos[(anos==ano)],function(x) fread(x,nrows = (-1),dec=",")))
  
  

  
  # fread(lista_arquivos[i],nrows = -1,select = c("CEI Vinculado","CBO Ocupação 2002",
  #                                                       "CNPJ / CEI","Data Admissão Declarada",
  #                                                       "Dia de Desligamento","PIS",
  #                                                       "Mês Desligamento"))
  base[,DA_DESLIGAMENTO_RAIS_MD:=ifelse(is.na(`Dia de Desligamento`),NA,
                                  paste0(`Dia de Desligamento`,"-",
                                         `Mês Desligamento`,"-",
                                         ano))]
  base[,DA_FINAL_PRIMEIRO_AFAS:=ifelse(is.na(`Dia Fim AF1`),NA,
                                        paste0(`Dia Fim AF1`,"-",
                                               `Mês Desligamento`,"-",
                                               ano))]
  base[,DA_FINAL_SEGUNDO_AFAS:=ifelse(is.na(`Dia Fim AF2`),NA,
                                        paste0(`Dia Fim AF2`,"-",
                                               `Mês Desligamento`,"-",
                                               ano))]
  base[,DA_FINAL_TERCEIRO_AFAS:=ifelse(is.na(`Dia Fim AF3`),NA,
                                        paste0(`Dia Fim AF3`,"-",
                                               `Mês Desligamento`,"-",
                                               ano))]
  base[,DA_INICIO_PRIMEIRO_AFAS:=ifelse(is.na(`Dia Ini AF1`),NA,
                                       paste0(`Dia Ini AF1`,"-",
                                              `Mês Desligamento`,"-",
                                              ano))]
  base[,DA_INICIO_SEGUND_AFAS:=ifelse(is.na(`Dia Ini AF2`),NA,
                                      paste0(`Dia Ini AF2`,"-",
                                             `Mês Desligamento`,"-",
                                             ano))]
  base[,DA_INICIO_TERCEIRO_AFAS:=ifelse(is.na(`Dia Ini AF3`),NA,
                                       paste0(`Dia Ini AF3`,"-",
                                              `Mês Desligamento`,"-",
                                              ano))]
  
  # corrigindo nome
  transnomevar <- read_excel("1files\\layout_nomes.xlsx",sheet = "Plan2")
  
  varschange <- transnomevar[(!is.na(transnomevar$RAIS_BRUTA)),]
  # names(base)
  # write.csv2(names(base),"1files\\xx.csv")
  # write.csv2(names(base),"1files\\xx.csv")
  ids <- which(varschange$RAIS_EST %in% names(base))
  
  setnames(base,varschange$RAIS_EST[ids],varschange$RAIS_BRUTA[ids])
  
  
  
  base[,(grep(x=names(base),pattern = "^VA",value = T)):= lapply(.SD, func_vl_rais_est), .SDcols = grep(x=names(base),pattern = "^VA",value = T)]
  base[,(grep(x=names(base),pattern = "^Vl",value = T)):= lapply(.SD, func_vl_rais_est), .SDcols = grep(x=names(base),pattern = "^Vl",value = T)]
  
  
  base[,tipo_base:=1]
  base[,IN_RETIFICACAO_VINCULO:=1]
  base[,CO_CREA:=1:.N]
  base[,CN_NUMERO_SEQ:=1]
  base[,ano:=ano]
  # names(base)
  
  vars_identificacao <- c("DA_ADMISSAO_RAIS_DMA","DA_DESLIGAMENTO_RAIS_MD","CO_CNPJ_CEI",
                          "CO_CEI_VINCULADO","CO_PIS","CO_CBO_RAIS")
  
  base[,id_duplicatas:=(.N),by=vars_identificacao]
  
  a <- base[,.(N = .N),by=c("id_duplicatas")]
  write.csv2(a,paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\EST\\a_",ano,".csv"))
  
  saveRDS(base[(id_duplicatas==1),],
          paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\EST\\BASE_1_",ano,".rds"))
  
  base <- base[(id_duplicatas!=1),]
  # 
  gc()
  
  vars_unique <- setdiff(names(base),
                         c("tipo_base","TIPO_RAIS_OPERACIONAL","CO_CREA",
                           "QT_VINCULO_ESTAB","CN_NUMERO_SEQ"))
  base[,id_duplicatas_unique:=.N,by=c(vars_unique)]
  b <- base[,.(N = .N),by=c("id_duplicatas","id_duplicatas_unique")]
  write.csv2(b,paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\EST\\b_",ano,".csv"))
  # B2: VINCULOS QUE DE FATO APRESENTAM DUPLICIDADE TANTO CONSIDENRANDO AS VARIÁVEIS BÁSICAS QUANTO PARA QUASE TODAS AS VARIÁVEIS DA BASE
  saveRDS(base[(id_duplicatas==id_duplicatas_unique),vars_unique,with=FALSE],
          paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\EST\\BASE_2_",ano,".rds"))
  
  base <- base[(id_duplicatas!=id_duplicatas_unique),]
  
  saveRDS(base,
          paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\EST\\BASE_3_",ano,".rds"))
  vars_unique <- setdiff(names(base),
                         c("tipo_base","TIPO_RAIS_OPERACIONAL","CO_CREA","CN_NUMERO_SEQ",
                           "QT_VINCULO_ESTAB",c(vars_identificacao,"id_duplicatas",
                                                "id_duplicatas_unique","ano")))
  
  
  vars_unique_numerico <- grep(x=vars_unique,pattern = "^VA|^ME|^QT|^Vl",value = T)
  vars_unique_n_numerico <- setdiff(vars_unique,vars_unique_numerico)
  
  
  base4 <- base[!(id_duplicatas==2 & id_duplicatas_unique==1),]
  if(nrow(base4)>0){
  base4[,id_base4_n_unica:=1:.N,by=c(vars_identificacao)]
  
  base4[,.N,by="id_base4_n_unica"]
  base4 <- base4[(id_base4_n_unica==1),]
  
  saveRDS(base4,
          paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\EST\\BASE_4_",ano,".rds"))
  }
  baset <- base[(id_duplicatas==2 & id_duplicatas_unique==1),]
  
  
  baset <- cbind(baset, zeroslinha = rowSums(baset[, vars_unique,with=FALSE]==0))
  baset[,maxzero:=max(zeroslinha),by=vars_identificacao]
  
  expr2 <-  paste0("b3<-baset[,.('",
                   paste(vars_unique_numerico,"'=length(unique('",vars_unique_numerico,"'))",
                         collapse = ",'",sep=""),
                   "),by=c('",paste0(c(vars_identificacao,"id_duplicatas",
                                       "id_duplicatas_unique",
                                       vars_unique_n_numerico),collapse = "','"),"')]")
  
  
  eval(parse(text = expr2))
  
  
  b3 <- cbind(b3, varsdiferentes = rowSums(b3[, vars_unique_numerico,with=FALSE]==2))
  setkeyv(b3,c(vars_identificacao,"id_duplicatas","id_duplicatas_unique",
               vars_unique_n_numerico))
  setkeyv(baset,c(vars_identificacao,"id_duplicatas","id_duplicatas_unique",
                  vars_unique_n_numerico))
  baset <- baset[b3[,c(vars_identificacao,"id_duplicatas","id_duplicatas_unique",
                       vars_unique_n_numerico,"varsdiferentes"),with=FALSE], nomatch=0]
  
  baset[,IN_FLAG_DUPLICADO_RETIFICADO_ZERO:=ifelse(varsdiferentes==(maxzero-zeroslinha) & IN_RETIFICACAO_VINCULO==1 & tipo_base==0,1,0),]
  baset[,VAR_IN_FLAG_DUPLICADO_RETIFICADO_ZERO:=var(IN_FLAG_DUPLICADO_RETIFICADO_ZERO),by=c(vars_identificacao,"id_duplicatas","id_duplicatas_unique",
                                                                                            vars_unique_n_numerico)]
  
  baset[,id_duplicatas_unique_zero:=(.N),by=c(vars_identificacao,"id_duplicatas","id_duplicatas_unique",
                                              vars_unique_n_numerico)]
  
  baset[,id_duplicatas_unique_zero_identificadora:=(.N),by=vars_identificacao]
  
  bd <- baset[,.(N = .N),by=c("id_duplicatas","id_duplicatas_unique","id_duplicatas_unique_zero",
                              "id_duplicatas_unique_zero_identificadora",
                              "VAR_IN_FLAG_DUPLICADO_RETIFICADO_ZERO","IN_FLAG_DUPLICADO_RETIFICADO_ZERO")]
  write.csv2(bd,paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\EST\\bd_",ano,".csv"))
  
  
  btemploop <- NULL
  vars <- vars_unique_numerico[1]
  for( vars in vars_unique_numerico){
    print(vars)
    btemploop <- rbindlist(list(btemploop,melt(b3[,.N,by=vars], id.vars = "N",
                                               measure.vars = vars)))
  }
  rm(b3)
  gc()
  
  write.csv2(btemploop,paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\EST\\btemploop_",ano,".csv"))
  
  baset_unica <- baset[(VAR_IN_FLAG_DUPLICADO_RETIFICADO_ZERO==0.5 & IN_FLAG_DUPLICADO_RETIFICADO_ZERO==1),]
  baset_n_unica <- baset[(VAR_IN_FLAG_DUPLICADO_RETIFICADO_ZERO!=0.5 | is.na(VAR_IN_FLAG_DUPLICADO_RETIFICADO_ZERO)),]
  
  baset_n_unica[,id_baset_n_unica:=1:.N,by=c(vars_identificacao)]
  baset_n_unica[,.N,by="id_baset_n_unica"]
  baset_n_unica <- baset_n_unica[(id_baset_n_unica==1),]
  
  baset_unica[,tipo_duplicata:="valor_rem_ou_hr"]
  baset_n_unica[,tipo_duplicata:="duplicatas_outras_vars"]
  names(baset_unica)
  names(baset_n_unica)
  
  
  baset_unica <- rbindlist(list(baset_unica,baset_n_unica),fill = TRUE)
  gc()
  
  rm(baset)
  rm(baset_n_unica)
  
  rm(base)
  gc()
  base2 <- readRDS(paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\EST\\BASE_2_",ano,".rds"))
  base2[,id_base2_n_unica:=1:.N,by=c(vars_identificacao)]
  
  base2[,.N,by="id_base2_n_unica"]
  base2 <- base2[(id_base2_n_unica==1),]
  
  if(nrow(base4)>0){
  basefinal <- rbindlist(list(baset_unica,
                              base4[,tipo_duplicata:="duplicata_nao_dupla_nao_resolvida"],
                              base2[,tipo_duplicata:="duplicatas_iguais"]),fill = TRUE)
  }else{
    basefinal <- rbindlist(list(baset_unica,
                                base2[,tipo_duplicata:="duplicatas_iguais"]),fill = TRUE)
    
  }
  
  
  base1 <- readRDS(paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\EST\\BASE_1_",ano,".rds"))  
  base1[,tipo_duplicata:="base_unica"]
  # rm(list = (ls())[-8])
  gc()
  saveRDS(rbindlist(list(basefinal,
                         base1),fill = TRUE),
          paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\EST\\BASE_RAIS_BRUTA_UNICA_",ano,".rds"))
}
