#Pacoes

library(data.table)
library(dplyr)
library(bit64)
library(lubridate)
library(stringr)
library(readxl)
library(tidyverse)
# limpando 
rm(list = ls())
gc()

base_salario <- data.frame(ano=c(2002:2020),
                           sl =c(200,
                                 240,260,300,350,380,415,
                                 465,510,545,622,678,
                                 724,788,880,937,954,
                                 1039,1045)) %>% as.data.table()


# variáveis identificadoras:
# "DA_ADMISSAO_RAIS_DMA","DA_DESLIGAMENTO_RAIS_MD","CO_CNPJ_CEI","CO_PIS","CO_CEI_VINCULADO","CO_CBO_RAIS"


# importando e rezumindo

caminho <- "\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA"
caminho_prog <- getwd()
lista_arquivos<-list.files(pattern = "^COMPLETO.B.*",caminho,full.names = T,recursive = F)
mapa <- read_excel(file.path(caminho_prog,"1files","Resumo_arquivos.xlsx"))
i <- 2

for(i in 9:1){
  print(i)
  #no prazo
  ano <- gsub(".*COMPLETO.B.(\\d{4}).rds","\\1",lista_arquivos[i])
  base <- readRDS(lista_arquivos[i])
  base[,tipo_base:=1]
  if(!is.na(mapa$FUNC_FORA_PRAZO[mapa$RAIS==ano])){
    
  
    # fora do prazo
    base_fora_prazo <- readRDS(file.path("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA",
                                         paste0("FORA_PRAZO_COMPLETO_B_",ano,".rds")))
    base_fora_prazo[,tipo_base:=0]
    
    
    # base <- bind_rows(base,base_fora_prazo)
    # rm(base_fora_prazo)
    # gc()
  }
  
  
  
  vars_identificacao <- c("DA_ADMISSAO_RAIS_DMA","DA_DESLIGAMENTO_RAIS_MD","CO_CNPJ_CEI",
                          "CO_CEI_VINCULADO","CO_PIS","CO_CBO_RAIS")
  
  base <- rbindlist(list(base,base_fora_prazo))
  rm(base_fora_prazo)
  gc()

  # registro sem duplicidade por variáveis identificadoras desconsiderando CBO
  
  base[,id_duplicatas:=(.N),by=vars_identificacao]
  base[,TIPO_RAIS_OPERACIONAL:=substr(CO_CREA,4,5)]
  
  # tabela "a" indica quantitativos iniciais de problemas a serem resolvidos
  a <- base[,.(N = .N),by=c("id_duplicatas")]
  write.csv2(a,paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\res_temp\\a_",ano,".csv"))
  
  # "B1: VÍNCULO ÚNICO APENAS PELAS VARIÁVEIS CHAVE"
  
  saveRDS(base[(id_duplicatas==1),],
          paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\BASE_1_",ano,".rds"))
  
  # REMOVENDO REGISTROS SEM PROBLEMAS
  base <- base[(id_duplicatas!=1),]
  # 
  gc()
  
  # RETIRANDO VARIÁVEIS QUE NÃO ENTRAM NA CHECAGEM DE DUPLIICIDADE
  vars_unique <- setdiff(names(base),
                         c("tipo_base","TIPO_RAIS_OPERACIONAL","CO_CREA",
                           "QT_VINCULO_ESTAB","CN_NUMERO_SEQ"))

    
  base[,id_duplicatas_unique:=.N,by=c(vars_unique)]
  # BASE "B" APRESENTA O QUANTITATIVO DE DUPLICIDADE SISTEMÁTICA PARA TODAS AS VARIÁVEIS 
  b <- base[,.(N = .N),by=c("id_duplicatas","id_duplicatas_unique")]
  write.csv2(b,paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\res_temp\\b_",ano,".csv"))
  # B2: VINCULOS QUE DE FATO APRESENTAM DUPLICIDADE TANTO CONSIDENRANDO AS VARIÁVEIS BÁSICAS, QUANTO PARA QUASE TODAS AS VARIÁVEIS DA BASE
  saveRDS(base[(id_duplicatas==id_duplicatas_unique),vars_unique,with=FALSE],
          paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\BASE_2_",ano,".rds"))
  
  
  
  # REMOVENDO TODOS OS CASOS EM QUE A DUPLICIDADE OCORRE PARA TODAS AS VARIÁVEIS.
  base <- base[(id_duplicatas!=id_duplicatas_unique),]

  
  saveRDS(base,
          paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\BASE_3_",ano,".rds"))

  # base3 <- readRDS(paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\BASE_3_",ano,".rds"))

  # 
  vars_unique <- setdiff(names(base),
                         c("tipo_base","TIPO_RAIS_OPERACIONAL","CO_CREA","CN_NUMERO_SEQ",
                           "QT_VINCULO_ESTAB",c(vars_identificacao,"id_duplicatas",
                                                "id_duplicatas_unique","ano")))

  #  IDENTIFICANDO APENAS VARIÁVEIS QUANTITATIVAS
  vars_unique_numerico <- grep(x=vars_unique,pattern = "^VA|^ME|^QT",value = T)
  vars_unique_n_numerico <- setdiff(vars_unique,vars_unique_numerico)
  
  # RESTRINGINDO ANÁLISE PARA CASOS EM QUE A DIVERGÊNCIA NÃO É RESTRITA NAS VARIÁVEIS NUMÉRICAS
  base4 <- base[!(id_duplicatas==2 & id_duplicatas_unique==1),]
  
  base4[,id_base4_n_unica:=1:.N,by=c(vars_identificacao)]
  
  base4[,.N,by="id_base4_n_unica"]
  
  # SELECIONADO O PRIMEIRO REGISTRO
  base4 <- base4[(id_base4_n_unica==1),]
  
  saveRDS(base4,
          paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\BASE_4_",ano,".rds"))
  
  # RESTRINGINDO ANÁLISE PARA CASOS EM QUE HÁ DUPLICIDADE NAS VARIÁVEIS IDENTIFICADORAS, MAS HÁ DIVERGÊNCIA NAS VARIÁVEIS NUMÉRICAS
  baset <- base[(id_duplicatas==2 & id_duplicatas_unique==1),]

  
  baset <- cbind(baset, zeroslinha = rowSums(baset[, vars_unique,with=FALSE]==0))
  baset[,maxzero:=max(zeroslinha),by=vars_identificacao]
  
  expr2 <-  paste0("b3<-baset[,.(",
                   paste(vars_unique_numerico,"=length(unique(",vars_unique_numerico,"))",
                         collapse = ",",sep=""),
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
  write.csv2(bd,paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\res_temp\\bd_",ano,".csv"))
  
  
  
  btemploop <- NULL
  vars <- vars_unique_numerico[1]
  for( vars in vars_unique_numerico){
  print(vars)
    btemploop <- rbindlist(list(btemploop,melt(b3[,.N,by=vars], id.vars = "N",
                                measure.vars = vars)))
  }
  rm(b3)
  gc()
  
  write.csv2(btemploop,paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\res_temp\\btemploop_",ano,".csv"))
  
  baset_unica <- baset[(VAR_IN_FLAG_DUPLICADO_RETIFICADO_ZERO==0.5 & IN_FLAG_DUPLICADO_RETIFICADO_ZERO==1),]
  baset_n_unica <- baset[(VAR_IN_FLAG_DUPLICADO_RETIFICADO_ZERO!=0.5 | is.na(VAR_IN_FLAG_DUPLICADO_RETIFICADO_ZERO)),]
  
  baset_n_unica[,id_baset_n_unica:=1:.N,by=c(vars_identificacao)]
  baset_n_unica[,.N,by="id_baset_n_unica"]
  # SELECIONADO O PRIMEIRO REGISTRO
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
  base2 <- readRDS(paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\BASE_2_",ano,".rds"))
  base2[,id_base2_n_unica:=1:.N,by=c(vars_identificacao)]

  base2[,.N,by="id_base2_n_unica"]
  base2 <- base2[(id_base2_n_unica==1),]
  
  
  basefinal <- rbindlist(list(baset_unica,
                              base4[,tipo_duplicata:="duplicata_nao_dupla_nao_resolvida"],
                              base2[,tipo_duplicata:="duplicatas_iguais"]),fill = TRUE)
  
  
  base1 <- readRDS(paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\BASE_1_",ano,".rds"))  
  base1[,tipo_duplicata:="base_unica"]

  gc()
  saveRDS(rbindlist(list(basefinal,
                         base1),fill = TRUE),
          paste0("\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA\\BASE_RAIS_BRUTA_UNICA_",ano,".rds"))
  
  
}


