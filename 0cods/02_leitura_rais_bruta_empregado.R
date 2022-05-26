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
library(stringr)
library(lubridate)


rm(list = ls())
gc()


caminho <- getwd()
destino <- "\\\\STORAGE6\\astec\\Estudos\\InovAtiva_Dados\\DADOS_RAIS_BRUTA"


## Dentro do prazo ==----
ano <- 2019
# nlimteste <- (-1)
nlimteste <- 10^3

func_leitura_raiz_prazo <- function(ano=2019,nlimteste=10^3,salva_rds=FALSE,base_memoria=TRUE,caminho=caminho,destino=destino){
  
  
  renamesdicio <- read_excel(file.path(caminho,"1docs","RaisBruta","layout_vinc_1999-2020_harmonizacao_variaveis.xlsx"))
  mapa <- read_excel(file.path(caminho,"1docs","RaisBruta","Resumo_arquivos.xlsx"))
  layoutano <- readRDS(file.path(caminho,"1docs","RaisBruta",paste0("layout_",ano,".rds")))
  if(ano==2009){
    # corrigindo CO_CNPJ_CEI e CO_CEI_VINCULADO
    layoutano_c <- readRDS(file.path(caminho,"1docs","RaisBruta",paste0("layout_",2012,".rds")))
    layoutano <- layoutano %>%
      left_join(layoutano_c %>% mutate(x=tamanho_caracters) %>% select(x,nomes_variaveis),
                by="nomes_variaveis")
    
    layoutano <- layoutano %>%
      mutate(tamanho_caracters = ifelse(tamanho_caracters>x,x,tamanho_caracters)) %>%
      select(-x)
  }
  
  vecs <- layoutano$tamanho_caracters
  nvecs <- layoutano$nomes_variaveis
  typvec <-  layoutano$tipo_variavel
  
  pasta_empr <- mapa$PASTA_EMPR[mapa$RAIS==ano]
  lista_arquivos<-list.files(pasta_empr,full.names = T,recursive = F)
  tamfile <- file.size(lista_arquivos)
  lista_arquivos <- lista_arquivos[tamfile>(10^7)]
  
  
  ab <- try(rbindlist(lapply(lista_arquivos,function(x) fread(file=x,nrows =  nlimteste,skip = 0,sep=";",header = FALSE,dec = ","))))
  
  if(class(ab)[1]!="try-error"){
    expr <- paste("ab<-ab[,':='(",
                  paste(ifelse(typvec=="N",ifelse(nvecs %in% c("CO_CNPJ_CEI","CO_CEI_VINCULADO",
                                                               "CO_PIS","CO_CPF",     
                                                               "CO_CREA","CO_PIS_INFO",
                                                               "CO_CNPJ_SIND_TRAB_BENE_CA_1","CO_CNPJ_SIND_TRAB_BENE_CA_2",
                                                               "CO_CNPJ_SIND_TRAB_BENE_CS","CO_CNPJ_SIND_TRAB_BENE_ASSI",
                                                               "CO_CNPJ_SIND_TRAB_BENE_CONF"),
                                                  paste(nvecs,"=as.integer64(substr(V1,",c(1,cumsum(vecs)[-length(vecs)]+1),",",
                                                        cumsum(vecs),"))"),
                                                  paste(nvecs,"=as.numeric(substr(V1,",c(1,cumsum(vecs)[-length(vecs)]+1),",",
                                                        cumsum(vecs),"))")),
                               paste(nvecs,"=substr(V1,",c(1,cumsum(vecs)[-length(vecs)]+1),",",
                                     cumsum(vecs),")")),
                        collapse = ",",sep=""),
                  ")]")
    
    ab <- try(eval(parse(text = expr)))
    
    if(class(ab)!="try-error"){
      
      renamesdicio <- renamesdicio %>% 
        mutate(nome_var_padrao = ifelse(nome_var_rais_bruta==nome_var_padrao,NA,nome_var_padrao))
      varschange <- renamesdicio$nome_var_padrao[renamesdicio$nome_var_rais_bruta %in% names(ab)]
      setnames(ab,names(ab)[!is.na(varschange)],varschange[!is.na(varschange)])
    }
  }
  ab[,ano:=ano]
  ab[,V1:=NULL]
  
  # corrigindo data de desligamento, adicionado ano
  ab[,DA_DESLIGAMENTO_RAIS_MD:=ifelse(DA_DESLIGAMENTO_RAIS_MD!=0,
                                      paste0(str_pad(DA_DESLIGAMENTO_RAIS_MD,pad="0",width = 4,side = "left"),ano),
                                      paste0(DA_DESLIGAMENTO_RAIS_MD))%>% dmy]
  ab[,DA_ADMISSAO_RAIS_DMA:=str_pad(DA_ADMISSAO_RAIS_DMA,pad="0",width = 8,side = "left")%>% dmy]
  vars_valores <- grep(names(ab),pattern = "^VA.*",value=TRUE)
  paste0(vars_valores,"=as.numeric(",vars_valores,")/100")
  expr2 <-  paste("ab<-ab[,':='(",
                  paste(vars_valores,"=as.numeric(",vars_valores,")/100",
                        collapse = ",",sep=""),
                  ")]")
  
  eval(parse(text = expr2))
  
  if(salva_rds){
  saveRDS(ab,file.path(destino,paste0("COMPLETO_B_",ano,".rds")))
    message(print(paste0("base da rais bruta de ",ano," com ",nrow(ab)," salva no caminho de destino indicado com sucesso.")))
  }
  if(base_memoria){
    show(paste0("base da rais bruta de ",ano," com ",nrow(ab)," linhas carregada na memória com sucesso."))
    return(ab)
  }else{
    return("base_memoria=FALSE)")
  }
}

func_leitura_raiz_fora_prazo <- function(ano=2019,nlimteste=10^3,salva_rds=FALSE,base_memoria=TRUE,caminho=caminho,destino=destino){
  
  
  renamesdicio <- read_excel(file.path(caminho,"1docs","RaisBruta","layout_vinc_1999-2020_harmonizacao_variaveis.xlsx"))
  mapa <- read_excel(file.path(caminho,"1docs","RaisBruta","Resumo_arquivos.xlsx"))
  layoutano <- readRDS(file.path(caminho,"1docs","RaisBruta",paste0("layout_",ano,".rds")))

  if(ano==2009){
    
    layoutano_c <- readRDS(file.path(caminho,"1docs","RaisBruta",paste0("layout_",2012,".rds")))
    layoutano <- layoutano %>%
      left_join(layoutano_c %>% mutate(x=tamanho_caracters) %>% select(x,nomes_variaveis),
                by="nomes_variaveis")
    
    layoutano <- layoutano %>%
      mutate(tamanho_caracters = ifelse(tamanho_caracters>x,x,tamanho_caracters)) %>%
      select(-x)
    
  }
  
  vecs <- layoutano$tamanho_caracters
  nvecs <- layoutano$nomes_variaveis
  typvec <-  layoutano$tipo_variavel
  
  pasta_empr <- mapa$FUNC_FORA_PRAZO[mapa$RAIS==ano]
  lista_arquivos<-list.files(pasta_empr,full.names = T,recursive = F)
  tamfile <- file.size(lista_arquivos)
  lista_arquivos <- lista_arquivos[tamfile>(10^7)]
  
  
  ab <- try(rbindlist(lapply(lista_arquivos,function(x) fread(file=x,nrows =  nlimteste,skip = 0,sep=";",header = FALSE,dec = ","))))
  
  if(class(ab)[1]!="try-error"){
    expr <- paste("ab<-ab[,':='(",
                                      paste(ifelse(typvec=="N",ifelse(nvecs %in% c("CO_CNPJ_CEI","CO_CEI_VINCULADO",
                                                                                   "CO_PIS","CO_CPF",     
                                                                                   "CO_CREA","CO_PIS_INFO",
                                                                                   "CO_CNPJ_SIND_TRAB_BENE_CA_1","CO_CNPJ_SIND_TRAB_BENE_CA_2",
                                                                                   "CO_CNPJ_SIND_TRAB_BENE_CS","CO_CNPJ_SIND_TRAB_BENE_ASSI",
                                                                                   "CO_CNPJ_SIND_TRAB_BENE_CONF"),
                                                                      paste(nvecs,"=as.integer64(substr(V1,",c(1,cumsum(vecs)[-length(vecs)]+1),",",
                                                                            cumsum(vecs),"))"),
                                                                      paste(nvecs,"=as.numeric(substr(V1,",c(1,cumsum(vecs)[-length(vecs)]+1),",",
                                                                            cumsum(vecs),"))")),
                                                   paste(nvecs,"=substr(V1,",c(1,cumsum(vecs)[-length(vecs)]+1),",",
                                     cumsum(vecs),")")),
                        collapse = ",",sep=""),
                  ")]")
    
    ab <- try(eval(parse(text = expr)))
    
    if(class(ab)!="try-error"){
      
      renamesdicio <- renamesdicio %>% 
        mutate(nome_var_padrao = ifelse(nome_var_rais_bruta==nome_var_padrao,NA,nome_var_padrao))
      varschange <- renamesdicio$nome_var_padrao[renamesdicio$nome_var_rais_bruta %in% names(ab)]
      setnames(ab,names(ab)[!is.na(varschange)],varschange[!is.na(varschange)])
      # saveRDS(base,file.path(caminho,"5temp",paste0("temp_",ano,".rds")))
    }
  }
  ab[,ano:=ano]
  ab[,V1:=NULL]
  
  # corrigindo data de desligamento, adicionado ano
  ab[,DA_DESLIGAMENTO_RAIS_MD:=ifelse(DA_DESLIGAMENTO_RAIS_MD!=0,
                                      paste0(str_pad(DA_DESLIGAMENTO_RAIS_MD,pad="0",width = 4,side = "left"),ano),
                                      paste0(DA_DESLIGAMENTO_RAIS_MD))%>% dmy]
  ab[,DA_ADMISSAO_RAIS_DMA:=str_pad(DA_ADMISSAO_RAIS_DMA,pad="0",width = 8,side = "left")%>% dmy]
  
  # CORREÇÃO DE VALORES SEM CASAS DECIMAIS
  vars_valores <- grep(names(ab),pattern = "^VA.*",value=TRUE)
  paste0(vars_valores,"=as.numeric(",vars_valores,")/100")
  expr2 <-  paste("ab<-ab[,':='(",
                  paste(vars_valores,"=as.numeric(",vars_valores,")/100",
                        collapse = ",",sep=""),
                  ")]")
  
  eval(parse(text = expr2))
  
  
  
  if(salva_rds){
    saveRDS(ab,file.path(destino,paste0("FORA_PRAZO_COMPLETO_B_",ano,".rds")))
    message(print(paste0("base da rais bruta fora do prazo de ",ano," com ",nrow(ab)," salva no caminho de destino indicado com sucesso.")))
  }
  if(base_memoria){
    show(paste0("base da rais bruta fora do prazo de ",ano," com ",nrow(ab)," linhas carregada na memória com sucesso."))
    return(ab)
  }else{
    return("base_memoria=FALSE)")
  }
}


for(ano in 2019:2011){
  print(ano)
  base <- func_leitura_raiz_prazo(ano,nlimteste = (-1),salva_rds = TRUE,
                                  base_memoria=FALSE,caminho,destino)
  
  base_fora_prazo <- func_leitura_raiz_fora_prazo(ano,nlimteste = (-1),salva_rds = TRUE,
                                       base_memoria=FALSE,caminho,destino)
  
}
