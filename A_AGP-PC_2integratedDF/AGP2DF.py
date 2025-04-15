# -*- coding: utf-8 -*-
"""
Created on Tue Apr 15 11:19:51 2025

@author: Theresa Rocco Pereira Barbosa

======================================================================
PADRONIZAÇÃO DE PROFUNDIDADES EM POÇOS DE EXPLORAÇÃO
======================================================================

Objetivo Principal:
-------------------
Padronizar a profundidade de múltiplos poços de explorção em relação ao nível do mar (NMM),
permitindo correlação precisa entre poços em diferentes elevações topográficas
na Bacia do Paraná.

Método:
-------
1. Extração automática de dados litológicos de arquivos AGP padrão ANP
2. Conversão de profundidades medidas para cotas absolutas em relação ao NMM
3. Consolidação em planilha única para análise integrada

Dados Críticos Processados:
--------------------------
- TOP_BAP/BOTTOM_BAP: Profundidades medidas do poço (em relação ao BAP)
- TOP_NM/BOTTOM_NM: Cotas absolutas em relação ao nível do mar
- TOP_DATUM/BOTTOM_DATUM: Cotas no datum local (Corrego Alegre/SAD69)
- Dados complementares: Coordenadas, litologia, equipamento de perfuração

Fluxo de Trabalho:
-----------------
1. parse_well_file(): Processa arquivos individuais
   - Extrai dados básicos (BAP, mesa rotativa, coordenadas)
   - Interpreta seção de litologia com expressões regulares
   - Calcula cotas absolutas (NMM) e relativas (datum)

2. process_well_files(): Consolida múltiplos arquivos
   - Varre diretório de dados
   - Padroniza estrutura de dados
   - Exporta para Excel

Exemplo de Saída:
----------------
| ID_POCO    | TOP_BAP | BOTTOM_BAP | TOP_NM | BOTTOM_NM | LITOLOGIA |
|------------|---------|------------|--------|-----------|-----------|
| 1AB 0001 SP| 4.0     | 11.0       | 493.7  | 486.7     | SILTITO   |

Observações Técnicas:
--------------------
1. O BAP (Boca do Poço) é a referência primária para medições
2. A mesa rotativa é usada apenas para controle operacional
3. O cálculo TOP_NM = BAP - TOP_BAP garante padronização absoluta
4. Compatível com diferentes datums (conversão implícita via BAP)

Aplicações:
----------
- Correlação estratigráfica regional
- Modelagem de reservatórios
- Integração com dados sísmicos
- Controle de qualidade em bancos de dados

Exemplo de cabeçalho de um arquivo AGP do poço 1AB-0001-SP da Bacia do Paraná
----------
 POCO           :    1AB  0001  SP       
                                                                                                                                     
 IDENTIFICADOR  :    SF22R44NO25H52D                         BACIA         :   PARANA                                                
 CAMPO          :                                           DATUM          :   CORREGO ALEGRE                                        
 LATITUDE       :   -22.77888 (  -22  46 44.0)              LONGITUDE      :   -48.18179 (  -48  10 54.4)                            
 MERID.CENTRAL  :   - 51                                    COORD.UTM FUNDO:   7478166.7S  789376.1W                                
 DISTRITO       :   E&P-SUL                                 ESTADO         :   SP                                                    
 MUNICIPIO      :   PIRACICABA                              CODIGO POCO    :   003813                                                
 QUADRICULA     :   SF22R4                                  MESA ROTATIVA  :    502.0                                                
 B.A.P          :   497.7                                   P.F.SONDADOR   :   1659.5                                                
 PROF.MX.PERF.  :   1660.7                                  METROS PERF.   :   1660.7                                                
 MAIOR PROF.    :   1660.7                                  INICIO         :   04/10/71                                              
 TERMINO        :   01/11/71                                DATA LIB.SONDA :   03/11/71                                              
 ULT.RECLASSIF. :   30/11/71                                ULT.RECLASSIF. :   LOCACAO                                               
 SITUACAO LOC.  :   CONCLUIDO                               DATA APROV.LOC :   14/04/71                                              
 CADASTRO       :   07083                                   SEDOC          :   0143                                                  
 SONDA          :   SC60                                    FORMACAO P.F.  :   EMBASA                                              
 TERRA / MAR    :   TERRA                                   ACS RISCO      :                                                         
 CONTR. RISCO   :                                           DOC.APROV.LOC. :   DEXPRO-T-38.162/71                                    
 NOME           :   ANHEMBI.1                               CLASSIFICACAO  :   PIONEIRO                                              
 RECLASSIFICACAO:   SECO SEM INDICACAO DE H.C.                                                                                       
 MC C.UTM BASE  :   - 51                                    COORD.UTM BASE :   7478166.7S    789376.1W                             
 DADOS  LOCACAO :                                                                                                                    
 MC LOCACAO     :   - 51                                    COORD.UTM LOC. :   7478200.0S   789350.0W                              
 DATUM LOCACAO  :   CORREGO ALEGRE                          EMPR. OPER.1/2 :   PETROBRAS             
"""

import os
import re
import pandas as pd

def parse_well_file(file_path):
    """
    Processa arquivo AGP individual, convertendo profundidades para:
        - TOP_NM: Cota do topo em relação ao nível do mar (BAP - TOP_BAP)
        - BOTTOM_NM: Cota da base em relação ao nível do mar (BAP - BOTTOM_BAP)
        Mantém os dados originais em relação ao BAP e datum local para referência cruzada.
    """
    with open(file_path, 'r', encoding='latin-1') as file:
        content = file.read()
    
    # Extração dos dados básicos
    mesa_rotativa = float(re.search(r'MESA ROTATIVA\s*:\s*([\d\.]+)', content).group(1))
    bap = float(re.search(r'B\.A\.P\s*:\s*([\d\.]+)', content).group(1))
    profundidade_total = float(re.search(r'METROS PERF\.\s*:\s*([\d\.]+)', content).group(1))
    
    well_id = re.search(r'POCO\s*:\s*(\S+\s+\S+\s+\S+)', content).group(1).strip()
    latitude = re.search(r'LATITUDE\s*:\s*([-\d\.]+)', content).group(1)
    longitude = re.search(r'LONGITUDE\s*:\s*([-\d\.]+)', content).group(1)
    
    # Seção de litologia
    litology_section = re.search(r'LITOLOGIA\s*-(.*?)(\+{3,}|RESUMO DAS ROCHAS)', content, re.DOTALL)
    if not litology_section:
        return None
    
    litology_lines = litology_section.group(1).split('\n')
    litology_data = []
    
    previous_base_boca = None
    previous_base_datum = None
    
    for line in litology_lines:
        if not line.strip():
            continue
        
        match_topo_base = re.match(
            r'^\s*(\d+\.?\d*)\s*\(\s*(-?\d+\.?\d*)\)\s+(\d+\.?\d*)\s*\(\s*(-?\d+\.?\d*)\)\s+\d*\s*(\w+)',
            line
        )
        match_base = re.match(
            r'^\s+(\d+\.?\d*)\s*\(\s*(-?\d+\.?\d*)\)\s+\d*\s*(\w+)',
            line
        )
        
        if match_topo_base:
            topo_boca = float(match_topo_base.group(1))
            topo_datum = float(match_topo_base.group(2))
            base_boca = float(match_topo_base.group(3))
            base_datum = float(match_topo_base.group(4))
            litologia = match_topo_base.group(5)
        elif match_base and previous_base_boca is not None:
            topo_boca = previous_base_boca
            topo_datum = previous_base_datum
            base_boca = float(match_base.group(1))
            base_datum = float(match_base.group(2))
            litologia = match_base.group(3)
        else:
            continue
        
        previous_base_boca = base_boca
        previous_base_datum = base_datum
        
        # Cálculos
        topo_nivel_mar = bap - topo_boca  # TOP_NM
        base_nivel_mar = bap - base_boca  # BOTTOM_NM

        # Estrutura de dados
        litology_data.append({
            'ID_POCO': well_id,
            # Profundidades em relação ao BAP
            'TOP_BAP': topo_boca,
            'BOTTOM_BAP': base_boca,
            # Cotas no datum
            'TOP_DATUM': topo_datum,
            'BOTTOM_DATUM': base_datum,
            # Cotas em relação ao nível do mar
            'TOP_NM': topo_nivel_mar,
            'BOTTOM_NM': base_nivel_mar,
            # Informações litológicas
            'LITOLOGIA': litologia,
            # Dados de referência
            'LATITUDE': float(latitude),
            'LONGITUDE': float(longitude),
            'MESA_ROTATIVA': mesa_rotativa,
            'BAP': bap,
            'PROF_TOTAL': profundidade_total
        })
    
    return litology_data


def process_well_files(directory):
    """
    Processa todos os arquivos .txt de uma pasta com dados de poços.
    """
    all_data = []
    
    for filename in os.listdir(directory):
        if filename.endswith(".txt"):
            file_path = os.path.join(directory, filename)
            data = parse_well_file(file_path)
            if data:
                all_data.extend(data)
    return pd.DataFrame(all_data)


# ===== Execução principal =====
input_path  = "C:/Users/Theresa/OneDrive/PESQUISA/LabMeg_Exxon/PMP_BC_WELL_LOGGING/A_AGP-PC_2integratedDF/AGP_data/"
output_path = "C:/Users/Theresa/OneDrive/PESQUISA/LabMeg_Exxon/PMP_BC_WELL_LOGGING/A_AGP-PC_2integratedDF/litologia_pocos.xlsx"

# Processa e exporta
df = process_well_files(input_path)
df.to_excel(output_path, index=False)