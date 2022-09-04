library(shinythemes)
library(shinyWidgets)
library(DT)
library(tidyverse)
library(janitor)
library(data.table)
library(shiny)
library(bs4Dash)
library(pool)
library(DBI)
library(RPostgres)
library(shinyvalidate)
library(shinyjs)
library(feather)
library(openssl)
library(reticulate)
library(uuid)
library(dplyr)
library(ggcharts)
library(ggplot2)
library(echarts4r)
library(lubridate)
library(fst)
library(collapse)
library(feather)
library(shinymanager)


db <- 'mozprocava'  
host_db <- "mozprocava.ckzqmdzjvnlv.us-east-1.rds.amazonaws.com"
db_port <- '5432'  
db_user <- "postgres"
db_password <- "MZ;PROCAVA"
pool <- dbPool(RPostgres::Postgres(), dbname = db, host=host_db, port=db_port, user=db_user, password=db_password)
onStop(function() {poolClose(pool)})


metas_granulares <- DBI::dbGetQuery(pool, "SELECT * FROM procava.granular_targets")
e_sistafe_ced <- read_feather("e_sistafe.feather", columns = c("esistafe_key", "e_sistafe_w_code"))
awpb_granular <- read_feather("granular_awpb_2022.feather")
awpb_granular <- merge(awpb_granular, e_sistafe_ced, by.x="ced", by.y="esistafe_key", all.x=TRUE)
# e_sistafe_ced <- read_feather("e_sistafe.feather")

metas_granulares <- setnames(metas_granulares, c("target", "beneficiarios_directos", "beneficiarios_indirectos", "pessoas_alcancadas","investment_mzn"),
                             c("Meta", "Beneficiarios directos", "Beneficiarios indirectos", "Membros dos agregados","Investment0 (MZN)"))

colunas <- sort(colnames(metas_granulares))
colunas <- sort(colnames(metas_granulares))

choice_valuechains <-  sort(unique(as.character(metas_granulares$value_chain)))
choices_actividades <-  sort(unique(as.character(metas_granulares$Actividade)))
choices_entidades <- sort(unique(as.character(metas_granulares$entidade_mader)))

District_admin <- sort(unique(as.character(metas_granulares$admin_distrito)))
Province_admin <- sort(unique(as.character(metas_granulares$admin_prov)))
Locality_admin <- sort(unique((metas_granulares$localidade_adm)))

choices_pilares <- sort(unique((metas_granulares$macro_intervention)))

culturas_choices <- sort(c("Alho" = "alho",
                           "Feijão Vulgar" = "beans",
                           "Mandioca" = "cassava", 
                           "Feijão Nhemba" = "cowpea",
                           "Amendoim" = "peanuts",
                           "Batata Reno" = "potato",
                           "Cebola" = "onion",
                           "Soja" = "soya",
                           "Gergelim" = "sesame",
                           "Batata Doce" = "sweetpotato"))

subcomponentes_choices <- sort(unique((metas_granulares$subcomponents)))

choices_colunas <- sort(c("Províncias" = "admin_prov",
                          "Distritos" = "admin_distrito",
                          "Localidade" = "localidade_adm",
                          "Ano fiscal" = "fiscal_year",
                          "Culturas" = "culturas",
                          "Cadeias de valor" = "value_chain",
                          "Intervenção" = "Actividade",
                          "Acção" = "descricao_da_actividade",
                          "Centro Zonal do IIAM" = "centro_zonal",
                          "Actividade (substantivo)" = "gov_description",
                          "Pilares" = "macro_intervention",
                          "Componentes" = "components",
                          "Subcomponentes" = "Subcomponents",
                          "Actividade e indicador" = "actividade_indicador",
                          "Infraestrutura" = "infrastructure",
                          "Propósito da infraestrutura" = "propositos_infraestruturas",
                          "Actividade (Inglês)" = "activivityunits_en",
                          "Actividade (Inglês)" = "activivityunits_en",
                          "Região provincial" = "province_region",
                          "ZAE Provincial" = "province_aez"))




awpb_granular <- merge(awpb_granular, e_sistafe_ced, by.x="ced", by.y="esistafe_key", all.x=TRUE)

# planned_pmu_spending
awpb_granular$components <- substr(awpb_granular$subcomponent, 1, 14)
awpb_granular$components <- gsub("Subcomponent", "Component", awpb_granular$components)
awpb_granular$disbursement_category <- ifelse(awpb_granular$pdr_category == "Consultancies", "III - Consultancies, Trainings and Workshops",
                                              ifelse(awpb_granular$pdr_category == "Salaries and Allowances", "V - Salaries and Allowances",
                                                     ifelse(awpb_granular$pdr_category == "Operating Costs", "VI - Operating Costs",
                                                            ifelse(awpb_granular$pdr_category == "Civil Works", "II - Civil Works",
                                                                   ifelse(awpb_granular$pdr_category == "Workshops", "III - Consultancies, Trainings and Workshops",
                                                                          ifelse(awpb_granular$pdr_category == "Consultancies", "III - Consultancies, Trainings and Workshops",
                                                                                 ifelse(awpb_granular$pdr_category == "Equipment and Materials", "I - Equipment and Materials",
                                                                                        ifelse(awpb_granular$pdr_category == "Trainings", "III - Consultancies, Trainings and Workshops",
                                                                                               ifelse(awpb_granular$pdr_category == "Credit Guarantee Funds", "IV - Credit Guarantee Funds",awpb_granular$pdr_category)))))))))

two_dimensions_pivot <- function(data, Description="Description", column_labs="column_labs", summary_value="summary_value") {
  library(expss)
  # data <- as.data.frame(data)
  out <- data %>% 
    group_by(Description, column_labs) %>%
    summarise(sales = sum(summary_value, na.rm = TRUE), .groups = 'drop') %>%
    pivot_wider(names_from = column_labs, values_from = sales) %>% adorn_totals("col")
  out <- out %>% adorn_totals("row")
  
  out_totals <- out %>% fsubset(Description == "Total")
  out_pct <- out_totals %>% mutate_if(is.numeric, .funs = ~./sum(data$summary_value)*100)
  out_pct$Description[out_pct$Description== "Total"] <- "Share (%)"
  out <- rbind(out, out_pct)
  out$`Share (%)` <- out$Total/sum(data$summary_value)*100
  return(out)
}

set_labels(
  language = "en",
  "Please authenticate" = "",
  "Username:" = "Usuário:",
  "Password:" = "Senha:",
  "Login" = "Aceder"
)

ui <- navbarPage("PROCAVA | IFR", collapsible = TRUE, inverse = TRUE,
                 
                 tabPanel("PROJECÇÕES", sidebarLayout( sidebarPanel(width = 3,
                                                                    
                                                                    selectizeInput("financiers_selected", "Financiadores", choices = c("Todos",
                                                                                                                                       "Donativo FIDA" = "IFAD Grant",
                                                                                                                                       "Crédito FIDA" = "IFAD Loan",
                                                                                                                                       "Donativo RPSF 1.ª Alocação" = "RPSF 1st Allocation",
                                                                                                                                       "RPSF 2.ª Alocação" =  "RPSF 2nd Allocation", 
                                                                                                                                       "Donativo CRI" = "CRI Grant",
                                                                                                                                       "Governo" = "Government",
                                                                                                                                       
                                                                                                                                       "Beneficiários" = "Beneficiaries"), selected = "Todos"),
                                                                    
                                                                    selectizeInput("variavel_linhas", "Variável das Linhas", choices = c("Componentes" = "componentnum_pt",
                                                                                                                                         "Categories do Acordo" = "disbursement",
                                                                                                                                         "Categorias PDR" = "pdr_category",
                                                                                                                                         "Subcomponentes" = "components_pt",
                                                                                                                                         "Financiadores" = "financier",
                                                                                                                                         "Importância" = "critical_path",
                                                                                                                                         "Ponto de Situação" = "en_status",
                                                                                                                                         "Trimestres" = "trimestre"
                                                                                                                                         # "Rubricas" = "e_sistafe_w_code"
                                                                    ), selected = "componentnum_pt"),
                                                                    
                                                                    selectizeInput("variavel_colunas", "Variável das Colunas", choices = c("Componentes" = "componentnum_pt",
                                                                                                                                           "Categories do Acordo" = "disbursement",
                                                                                                                                           "Categorias PDR" = "pdr_category",
                                                                                                                                           "Financiadores" = "financier", 
                                                                                                                                           "Subcomponentes" = "components_pt",
                                                                                                                                           "Importância" = "critical_path",
                                                                                                                                           "Ponto de Situação" = "en_status",
                                                                                                                                           "Trimestres" = "trimestre"
                                                                    ), selected = "trimestre")),
                                                       
                                                       mainPanel(DT::dataTableOutput("projected_budget")))),
                 

                 tabPanel("DESPESAS", sidebarLayout( sidebarPanel(width = 3,
                                                                  
                                                                  selectizeInput("financiers_selected_despesas", "Financiadores", choices = c("Todos",
                                                                                                                                              "Donativo FIDA" = "IFAD Grant",
                                                                                                                                              "Crédito FIDA" = "IFAD Loan",
                                                                                                                                              "Donativo RPSF 1.ª Alocação" = "RPSF 1st Allocation",
                                                                                                                                              "RPSF 2.ª Alocação" =  "RPSF 2nd Allocation", 
                                                                                                                                              "Donativo CRI" = "CRI Grant",
                                                                                                                                              "Governo" = "Government",
                                                                                                                                              "Beneficiários" = "Beneficiaries"), selected = "Todos"),
                                                                  
                                                                  selectizeInput("variavel_linhas_despesas", "Variável das Linhas", choices = c( "Rubricas"= "e_sistafe_pt", 
                                                                                                                                                 "Sector Proponente"= "sectores",           
                                                                                                                                                 "Centro de Custos (UGB)"= "cost_centers",             
                                                                                                                                                 "Tipo de Processo"= "process_type",
                                                                                                                                                 "categoria Acordo"= "disbursement",             
                                                                                                                                                 "Categoria PDR"= "pdr_category",    
                                                                                                                                                 "Componentes"= "componentes",      
                                                                                                                                                 "Subcomponentes"= "subcomponentes",    
                                                                                                                                                 "Situação"= "en_status",                
                                                                                                                                                 "Relevância"= "relevance",          
                                                                                                                                                 "Financiador"= "financiers",           
                                                                                                                                                 "Financiador (Detalhes)"= "financiers_detailed",
                                                                                                                                                 "Trimestres"= "fiscal_quarters",
                                                                                                                                                 "Ano Fiscal"= "fiscal_year"
                                                                  ), selected = "componentes"),
                                                                  
                                                                  selectizeInput("variavel_colunas_despesas", "Variável das Colunas", choices = c( "Rubricas"= "e_sistafe_pt", 
                                                                                                                                                   "Sector Proponente"= "sectores",           
                                                                                                                                                   "Centro de Custos (UGB)"= "cost_centers",             
                                                                                                                                                   "Tipo de Processo"= "process_type",
                                                                                                                                                   "Categoria Acordo"= "disbursement",             
                                                                                                                                                   "Categoria PDR"= "pdr_category",    
                                                                                                                                                   "Componentes"= "componentes",      
                                                                                                                                                   "Subcomponentes"= "subcomponentes",    
                                                                                                                                                   "Situação"= "en_status",                
                                                                                                                                                   "Relevância"= "relevance",          
                                                                                                                                                   "Financiador"= "financiers",           
                                                                                                                                                   "Financiador (Detalhes)"= "financiers_detailed",
                                                                                                                                                   "Trimestres"= "fiscal_quarters",
                                                                                                                                                   "Ano Fiscal"= "fiscal_year"
                                                                  ), selected = "fiscal_quarters"),
                                                                  
                                                                  selectizeInput("ano_fiscal", "Ano fiscal", choices = c("Todos", "2021", "2022", "2023", "2024", "2025")),
                                                                  
                                                                  selectizeInput("pmu_spending", "Unidade de Gestão", choices = c("Todas", "UNGP", "URGPS", "UPGPN", "URGPC", "URGPN")),
                                                                  
                                                                  dateRangeInput("periodo_despesa", "Período de Realização:",
                                                                                 start = "2022-01-01",
                                                                                 end   = "2022-12-31",
                                                                                 min = "2020-05-01",
                                                                                 max   = "2025-06-31")),

                                                     mainPanel(DT::dataTableOutput("expenditures_incurred")))),
                 
                 navbarMenu("PLANIFICAÇÃO",
                            tabPanel("PLANO 2022", sidebarLayout(sidebarPanel(width = 3,
                                                                                         
                                                                                         # selectizeInput("financiers_selected_planos", "Financiadores", choices = c("Todos",
                                                                                         #                                                                           "Donativo FIDA" = "IFAD Grant",
                                                                                         #                                                                           "Crédito FIDA" = "IFAD Loan",
                                                                                         #                                                                           "Donativo RPSF 1.ª Alocação" = "RPSF 1st Allocation",
                                                                                         #                                                                           "RPSF 2.ª Alocação" =  "RPSF 2nd Allocation", 
                                                                                         #                                                                           "Donativo CRI" = "CRI Grant",
                                                                                         #                                                                           "Governo" = "Government",
                                                                                         #                                                                           "Beneficiários" = "Beneficiaries"), selected = "Todos"),
                                                                                         # 
                                                                                         selectizeInput("variavel_linhas_planos", "Variável das Linhas", choices = c("Componentes" = "components",
                                                                                                                                                                     "Categories do Acordo" = "disbursement_category",
                                                                                                                                                                     "Categorias PDR" = "pdr_category",
                                                                                                                                                                     "Subcomponentes" = "subcomponent",
                                                                                                                                                                     "Financiadores" = "financiers",
                                                                                                                                                                     "Trimestres" = "fiscal_quarters",
                                                                                                                                                                     "Rubricas" = "e_sistafe_w_code"
                                                                                         ), selected = "components"),
                                                                               
                                                                               
                                                                               
                                                                               
                                                                               
                                                                                         
                                                                                         selectizeInput("variavel_colunas_planos", "Variável das Colunas", choices = c("Componentes" = "components",
                                                                                                                                                                       "Categories do Acordo" = "disbursement_category",
                                                                                                                                                                       "Categorias PDR" = "pdr_category",
                                                                                                                                                                       "Subcomponentes" = "subcomponent",
                                                                                                                                                                       "Financiadores" = "financiers",
                                                                                                                                                                       "Trimestres" = "fiscal_quarters"
                                                                                         ), selected = "fiscal_quarters"),

                                                                               selectizeInput("planned_pmu_spending", "Unidade de Gestão", choices = c("Todas", "UNGP"="NPMU", "URGPS"="SRPMU", "UPGPN"="NPPMU", "URGPC"="CRPMU", "URGPN"="NRPMU")),
                                                                               selectizeInput("relevance_planned", "Relevância", choices = c("Todas", "Actividade Simples"="Activity", "Actividade Composta"="Macro-activity", "Sub-actividade"="Sub-activity", "Lote"="Lot")),
                                                                               selectizeInput("relevance_quarters", "Trimestres", choices = c("Todos", "Q1", "Q2", "Q3","Q4")),
                                                                               selectizeInput("category_as_planned", "PDR Category", choices = c("Todas", unique(awpb_granular$pdr_category))),
                                                                               selectizeInput("subcomponents_as_planned", "Subcomponentes", choices = c("Todas", unique(awpb_granular$subcomponent))),
                                                                               selectizeInput("rubricas_as_planned", "Rubricas (CED)", choices = c("Todas", unique(e_sistafe_ced$e_sistafe_w_code))),



                                                                               selectizeInput("financiers_selected_planos", "Financiadores", choices = c("Todos",
                                                                                                                                                  "Donativo FIDA" = "IFAD Grant",
                                                                                                                                                  "Crédito FIDA" = "IFAD Loan",
                                                                                                                                                  "Donativo RPSF 1.ª Alocação" = "RPSF 1st Allocation",
                                                                                                                                                  "RPSF 2.ª Alocação" =  "RPSF 2nd Allocation",
                                                                                                                                                  "Donativo CRI" = "CRI Grant",
                                                                                                                                                  "Governo" = "Government",
                                                                                                                                                  "Beneficiários" = "Beneficiaries"), selected = "Todos"),

                                                                               dateRangeInput("periodo_planificado", "Período de Referência:",
                                                                                              start = "2022-01-01",
                                                                                              end   = "2023-01-01",
                                                                                              min = "2020-05-01",
                                                                                              max   = "2025-01-01")
                                                                               
                                                                               
                                                                               
                                                                               
                                                                               ),
                                                                            
                                                                            mainPanel(DT::dataTableOutput("planned_budget")))),
                            
                            
                            tabPanel("METAS 2020|2025", sidebarLayout(sidebarPanel(width = 2,
                                                                                   uiOutput("provinces_admin"),
                                                                                   uiOutput("districts_admin"),
                                                                                   uiOutput("localities_admin"),
                                                                                   selectInput(inputId ="variavel_linha", label = "Variável das linhas", choices = choices_colunas, selected = "actividade_indicador"),
                                                                                   selectInput(inputId ="variavel_coluna", label = "Variável das colunas", choices = choices_colunas, selected = "fiscal_year"),
                                                                                   varSelectInput(inputId ="numeric_variable", label ="Variável numérica", metas_granulares[, c("Meta", "Beneficiarios directos", "Beneficiarios indirectos", "Membros dos agregados","Investment0 (MZN)")]),
                                                                                   selectInput(inputId ="actividades", label = "Actividades", choices = c("Todas", choices_actividades), selected = "Todas", multiple = TRUE),
                                                                                   selectInput(inputId ="culturas", label ="Culturas", c("Todas", culturas_choices), selected = "Todas", multiple = TRUE),
                                                                                   selectInput(inputId ="cadeias", label ="Cadeias de Valor", c(choice_valuechains), selected = "Todas", multiple = TRUE),
                                                                                   selectInput(inputId ="pilares", label ="Pilares", c("Todos", choices_pilares), selected = "Todos"),
                                                                                   selectInput(inputId ="entidades_mader", label ="Entidades", c("Todas", choices_entidades), selected = "Todas"),
                                                                                   selectInput(inputId ="subcompon", label ="Componentes", c("Todas", subcomponentes_choices), selected = "Todas", multiple = TRUE),
                                                                                   # selectInput(inputId ="numeric_variable", label ="Componentes", c("Todas", subcomponentes_choices), selected = "Todas", multiple = TRUE),
                                                                                   selectInput(inputId ="anos", label ="Ano Fiscal", c("Todos", c(2021, 2022, 2023, 2024, 2025)), selected = "Todos", multiple = TRUE)),
                                                                      
                                                                      mainPanel(DT::dataTableOutput('targets_summary'), width = 10)))))

ui <- secure_app(
  ui = ui,
  tags_top = 
    tags$div(
      tags$h4("", style = "align:center"),
      tags$a(href='https://pmu.mozprocava.org',
             tags$img(src="PROGRAMME_LOGO.png", height = '127.5', width = '142.5'),
             '', target="_blank")
    ),
  # add information on bottom ?
  tags_bottom = tags$div(
    tags$p("", tags$a(href = "mailto:info@procava.gov.mz?Subject=Shiny%20aManager",target="_top", "Solicitar Assistência")),
    tags$p("", tags$a(href = " https://farfp.shinyapps.io/user_registration", target="_top", "Actualizar Credenciais"))
  )
)


server <- function(input, output, session) {
  
  
  linhas <- reactive({input$variavel_linha})
  colunas <- reactive({input$variavel_coluna})
  variavel_numerica <- reactive({input$numeric_variable})
  
  
  output$provinces_admin <- renderUI({
    selectizeInput("provincias_admin", "PROVÍNCIA", choices = c("Todas", Province_admin), selected = "Todas")
  })
  
  ## input dependant on the choices in `data1`
  output$districts_admin <- renderUI({
    selectizeInput("distritos_admin", "Distrito", choices = c(metas_granulares$admin_distrito[metas_granulares$admin_prov == input$provincias_admin], "Todos"), selected="Todos")
  })
  
  
  output$localities_admin <- renderUI({
    selectizeInput("localidades_admin", "Localidade", choices = c(metas_granulares$localidade_adm[metas_granulares$admin_distrito == input$distritos_admin], "Todas"),  selected = "Todas")
  })   
  
  
  heirarchy<-c("All", Province_admin)
  observe({
    hei1<-input$Province_admin
    hei2<-input$District_admin
    hei3<-input$Locality_admin
    
    choice1<-c("NONE",setdiff(heirarchy,c(hei2,hei3)))
    choice2<-c("NONE",setdiff(heirarchy,c(hei1,hei3)))
    choice3<-c("NONE",setdiff(heirarchy,c(hei1,hei2)))
    
    updateSelectInput(session,"heir1",choices=choice1,selected=hei1)
    updateSelectInput(session,"heir2",choices=choice2,selected=hei2)
    updateSelectInput(session,"heir3",choices=choice3,selected=hei3)
    
  })
  
  res_auth <- secure_server(check_credentials = check_credentials(credentials))
  credentials <-  DBI::dbGetQuery(pool, "SELECT * FROM admin.full_system_users")
  setnames(credentials, c("user_name", "user_password", "is_admin", "user_start", "user_expire"), c("user", "password", "admin", "start", "expire"))
  creds_reactive <- reactive({reactiveValuesToList(res_auth)}) 

  projected_budget <- reactive({
    projected_budget <- dbGetQuery(pool, "SELECT * FROM fiduciary.ifr_projections_full WHERE relevance NOT IN ('Lotes', 'Sub-activity')")
    projected_budget <- projected_budget %>% pivot_longer(cols = c(ifadgrant_q1:beneficiaries_q4), names_to = "financiers", values_to = "ammount") %>% dplyr::filter(ammount > 0)
    
    projected_budget <- projected_budget %>% separate("financiers", into = c("financiador", "trimestre"), sep = "_")
    
    projected_budget$trimestre <- toupper(projected_budget$trimestre)
    
    projected_budget$financier <- ifelse(projected_budget$financiador == "ifadgrant", "IFAD Grant",
                                         ifelse(projected_budget$financiador == "ifadloan", "IFAD Loan",
                                                ifelse(projected_budget$financiador == "rpsf1", "RPSF 1st Allocation",
                                                       ifelse(projected_budget$financiador == "rpsf2", "RPSF 2nd Allocation",
                                                              ifelse(projected_budget$financiador == "government", "Government",
                                                                     ifelse(projected_budget$financiador == "beneficiaries", "Beneficiaries", ifelse(projected_budget$financiador)))))))
    if(input$financiers_selected != "Todos"){projected_budget <- fsubset(projected_budget, financier %in% input$financiers_selected)}
    projected_budget
  })
  
  expenditures <- reactive({
    payments_dataset <- dbGetQuery(pool, "SELECT * FROM fiduciary.full_payments_dataset")
    payments_dataset<- payments_dataset %>% pivot_longer(cols = c(ifadgrant_pct:governmentmoney_pct), names_to = "financiers", values_to = "percentage_financed") %>% dplyr::filter(percentage_financed > 0)
    
    payments_dataset<- payments_dataset %>%
      mutate(fiscal_year = year(payments_dataset$submission_date), fiscal_quarters = quarters(payments_dataset$submission_date),
             usd_paid = payments_dataset$usd_paid*payments_dataset$percentage_financed/100,
             financiers_detailed = case_when(
               financiers == "ifadgrant_pct" ~ "IFAD Grant",
               financiers == "ifadloan_pct" ~ "IFAD Loan",
               financiers == "ifadrpsf1_pct" ~ "RPSF 1st Allocation",
               financiers == "ifadrpsf2_pct" ~ "RPSF 2nd Allocation",
               financiers == "beneficiaryinkind_pct" ~ "Beneficiaries (In-kind)",
               financiers == "beneficiarymonetary_pct" ~ "Beneficiaries (Monetary)",
               financiers == "privateinkind_pct" ~ "Private (In-kind)",
               financiers == "privatemoney_pct" ~ "Private (Monetary)",
               financiers == "governmentinkind_pct" ~ "Government (In-kind)",
               financiers == "governmentmoney_pct" ~ "Government (Monetary)",
               financiers == "cri_pct" ~ "CRI Grant",
               TRUE ~ "Outros"
             )
      ) %>% 
      mutate(
        financiers = case_when(
          financiers == "ifadgrant_pct" ~ "IFAD Grant",
          financiers == "ifadloan_pct" ~ "IFAD Loan",
          financiers == "ifadrpsf1_pct" ~ "RPSF 1st Allocation",
          financiers == "ifadrpsf2_pct" ~ "RPSF 2nd Allocation",
          financiers == "beneficiaryinkind_pct" ~ "Beneficiaries",
          financiers == "beneficiarymonetary_pct" ~ "Beneficiaries",
          financiers == "privateinkind_pct" ~ "Beneficiaries",
          financiers == "privatemoney_pct" ~ "Beneficiaries",
          financiers == "governmentinkind_pct" ~ "Government",
          financiers == "governmentmoney_pct" ~ "Government",
          financiers == "cri_pct" ~ "CRI Grant",
          TRUE ~ "Outros"
        )
      )
    
    if(input$financiers_selected_despesas != "Todos"){payments_dataset <- fsubset(payments_dataset, financiers %in% input$financiers_selected_despesas)}
    if(input$ano_fiscal != "Todos"){payments_dataset <- fsubset(payments_dataset, fiscal_year %in% input$ano_fiscal)}
    if(input$pmu_spending != "Todas"){payments_dataset <- fsubset(payments_dataset, cost_centers %in% input$pmu_spending)}
    
    payments_dataset <- payments_dataset %>% filter(submission_date >= input$periodo_despesa[1] & submission_date <= input$periodo_despesa[2])
    payments_dataset
  })
  
  output$projected_budget <- DT::renderDataTable({
    DT <- projected_budget() %>% select(Description = input$variavel_linhas, column_labs = input$variavel_colunas, summary_value = ammount)
    DT <- two_dimensions_pivot(DT)
    # DT  <- as.data.frame(DT)
    datatable(DT, rownames = FALSE, extensions = "Buttons", options = list(paging = TRUE, scrollX=TRUE, searching = TRUE, ordering = TRUE, dom = 'Bfrtip',
                                                                           
                                                                           buttons = c('copy', 'csv', 'excel', 'pdf'),
                                                                           pageLength=10,
                                                                           lengthMenu=c(5,10,20,100))) %>%
      formatCurrency(c(2:ncol(DT)), '')
  })
  

  
  output$expenditures_incurred <- DT::renderDataTable({
    DT <- expenditures() %>% select(Description = input$variavel_linhas_despesas, column_labs = input$variavel_colunas_despesas, summary_value = usd_paid)
    DT <- two_dimensions_pivot(DT)
    # DT  <- as.data.frame(DT)
    datatable(DT, rownames = FALSE, extensions = "Buttons", options = list(paging = TRUE, scrollX=TRUE, searching = TRUE, ordering = TRUE, dom = 'Bfrtip',
                                                                           
                                                                           buttons = c('copy', 'csv', 'excel', 'pdf'),
                                                                           pageLength=10,
                                                                           lengthMenu=c(5,10,20,100))) %>%
      formatCurrency(c(2:ncol(DT)), '')
  })
  

  awpb_granular <- reactive({
    
     e_sistafe_ced <- read_feather("e_sistafe.feather", columns = c("esistafe_key", "e_sistafe_w_code"))
     awpb_granular <- read_feather("granular_awpb_2022.feather")

     awpb_granular <- merge(awpb_granular, e_sistafe_ced, by.x="ced", by.y="esistafe_key", all.x=TRUE)

     # planned_pmu_spending
     awpb_granular$components <- substr(awpb_granular$subcomponent, 1, 14)
     awpb_granular$components <- gsub("Subcomponent", "Component", awpb_granular$components)
     awpb_granular$disbursement_category <- ifelse(awpb_granular$pdr_category == "Consultancies", "III - Consultancies, Trainings and Workshops",
                                                   ifelse(awpb_granular$pdr_category == "Salaries and Allowances", "V - Salaries and Allowances",
                                                          ifelse(awpb_granular$pdr_category == "Operating Costs", "VI - Operating Costs",
                                                                 ifelse(awpb_granular$pdr_category == "Civil Works", "II - Civil Works",
                                                                        ifelse(awpb_granular$pdr_category == "Workshops", "III - Consultancies, Trainings and Workshops",
                                                                               ifelse(awpb_granular$pdr_category == "Consultancies", "III - Consultancies, Trainings and Workshops",
                                                                                      ifelse(awpb_granular$pdr_category == "Equipment and Materials", "I - Equipment and Materials",
                                                                                             ifelse(awpb_granular$pdr_category == "Trainings", "III - Consultancies, Trainings and Workshops",
                                                                                                    ifelse(awpb_granular$pdr_category == "Credit Guarantee Funds", "IV - Credit Guarantee Funds",awpb_granular$pdr_category)))))))))

    if(input$financiers_selected_planos !="Todos"){awpb_granular <- awpb_granular[awpb_granular$financiers %in% input$financiers_selected_planos,]}
    if(input$planned_pmu_spending !="Todas"){awpb_granular <- awpb_granular[awpb_granular$programme_units %in% input$planned_pmu_spending,]}
    if(input$relevance_planned !="Todas"){awpb_granular <- awpb_granular[awpb_granular$relevance %in% input$relevance_planned,]}
    if(input$relevance_quarters !="Todos"){awpb_granular <- awpb_granular[awpb_granular$fiscal_quarter %in% input$relevance_quarters,]}
    if(input$category_as_planned !="Todas"){awpb_granular <- awpb_granular[awpb_granular$pdr_category %in% input$category_as_planned,]}
    if(input$subcomponents_as_planned !="Todas"){awpb_granular <- awpb_granular[awpb_granular$programme_units %in% input$subcomponents_as_planned,]}
    if(input$rubricas_as_planned !="Todas"){awpb_granular <- awpb_granular[awpb_granular$e_sistafe_w_code %in% input$rubricas_as_planned,]}
    awpb_granular <- awpb_granular %>% dplyr::filter(start_day >= input$periodo_planificado[1] & start_day  <= input$periodo_planificado[2])

    awpb_granular
  })
  
  
  metas_filtradas <- reactive({

    metas_granulares <- metas_granulares

    if(input$actividades !="Todas"){metas_granulares <- metas_granulares[metas_granulares$Actividade %in% input$actividades,]}
    if(input$cadeias !="Todas"){metas_granulares <- metas_granulares[metas_granulares$value_chain %in% input$cadeias,]}
    if(input$subcompon !="Todas"){metas_granulares <- metas_granulares[metas_granulares$subcomponents %in% input$subcompon,]}

    if(input$provincias_admin !="Todas"){metas_granulares <- metas_granulares[metas_granulares$admin_prov == input$provincias_admin,]}
    if(input$distritos_admin !="Todos"){metas_granulares <- metas_granulares[metas_granulares$admin_distrito == input$distritos_admin,]}
    if(input$localidades_admin !="Todas"){metas_granulares <- metas_granulares[metas_granulares$localidade_adm == input$localidades_admin,]}
    if(input$pilares !="Todos"){metas_granulares <- metas_granulares[metas_granulares$macro_intervention == input$pilares,]}
    if(input$entidades_mader !="Todas"){metas_granulares <- metas_granulares[metas_granulares$entidade_mader == input$entidades_mader,]}
    if(input$anos !="Todos"){metas_granulares <- metas_granulares[metas_granulares$fiscal_year == input$anos,]}

    metas_granulares
  })
  
  output$targets_summary <- DT::renderDataTable({
    
    dataset_selecionado <- metas_filtradas()[, c("Meta", "Beneficiarios directos", "Beneficiarios indirectos", "Membros dos agregados","Investment0 (MZN)", colunas(), linhas())]
    sumario <- dataset_selecionado %>% group_by_(input$variavel_linha, input$variavel_coluna) %>% summarise(meta = sum(!!!input$numeric_variable))
    sumario <- sumario %>% pivot_wider(values_from = "meta", names_from = input$variavel_coluna)
    sumario[is.na(sumario)] <- 0
    
    sumario <- sumario %>% adorn_totals("col") %>% adorn_totals("row")
    sumario <- sumario %>% fsubset(Total >0)
    sumario <- sumario %<>% rename("Descrição" = 1)
    
    sumario <- datatable(sumario, rownames = FALSE, extensions = 'Buttons', options = list(dom = "Blfrtip", 
                                                                         buttons = list("copy", list(extend = "collection", buttons = c("csv", "excel", "pdf"), text = "Download")), 
                                                                         lengthMenu = list( c(10, 20, -1), c(10, 20, "All")), pageLength = 10
    )) %>% 
      formatCurrency(c(2:ncol(sumario)), '')
  })
  
  output$planned_budget <- DT::renderDataTable({

    sumario <-   awpb_granular() %>% group_by_(input$variavel_linhas_planos, input$variavel_colunas_planos) %>% summarise(meta = sum(granularusd))
    sumario <- sumario %>% pivot_wider(values_from = "meta", names_from = input$variavel_colunas_planos)
    sumario[is.na(sumario)] <- 0
    sumario <- sumario %>% adorn_totals("col") %>% adorn_totals("row")
    sumario <- sumario %<>% rename("Descrição" = 1)
    
    sumario <- datatable(sumario, rownames = FALSE, extensions = 'Buttons', options = list(dom = "Blfrtip", 
                                                                                           buttons = list("copy", list(extend = "collection", buttons = c("csv", "excel", "pdf"), text = "Download")), 
                                                                                           lengthMenu = list( c(10, 20, -1), c(10, 20, "All")), pageLength = 10
    )) %>% 
      formatCurrency(c(2:ncol(sumario)), '')
  })

}

shinyApp(ui, server)