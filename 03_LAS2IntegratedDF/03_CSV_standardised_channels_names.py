# -*- coding: utf-8 -*-
"""
Created on Sat Apr 12 18:21:25 2025

@author: Theresa Rocco Pereira Barbosa
"""

import os
import pandas as pd
from tqdm import tqdm

# Configurações de caminhos
INPUT_DIR  = r"C:\Users\Theresa\OneDrive\PESQUISA\LabMeg_Exxon\PMP_BC_WELL_LOGGING\03_LAS2IntegratedDF\DF_output_02"
OUTPUT_DIR = r"C:\Users\Theresa\OneDrive\PESQUISA\LabMeg_Exxon\PMP_BC_WELL_LOGGING\03_LAS2IntegratedDF\DF_output_03"

# =============================================================================
# DICIONÁRIO COMPLETO DE PADRONIZAÇÃO DE CANAIS
# =============================================================================

CHANNEL_MAPPING = {
    # Profundidade e identificação
    'DEPT': 'DEPTH',
    'MD': 'DEPTH',
    
    # Raios Gama
    'GR': 'GR',
    'GR_1': 'GR',
    'GR_2': 'GR',
    'GRT': 'GR',
    'RGSS': 'GR_SHALLOW',
    'SGR': 'GR_SHALLOW',
    'SGR_1': 'GR_SHALLOW',
    'CGR': 'GR_COMPENSATED',
    'CGR_1': 'GR_COMPENSATED',
    'RHGR': 'GR_HYDROGEN',
    'HGR': 'GR_HYDROGEN',
    'HGR_1': 'GR_HYDROGEN',
    
    # Potencial Espontâneo
    'SP': 'SP',
    'SP_1': 'SP',
    'SP_2': 'SP',
    
    # Resistividade
    'ILD': 'RES_DEEP',
    'ILD_1': 'RES_DEEP',
    'ILD_2': 'RES_DEEP',
    'RILD': 'RES_DEEP',
    'ILT': 'RES_DEEP',
    'RLN': 'RES_MEDIUM',
    'RSN': 'RES_SHALLOW',
    'RLL': 'RES_LATEROLOG',
    'RLL_1': 'RES_LATEROLOG',
    'LLD': 'RES_LATEROLOG_DEEP',
    'LLD_1': 'RES_LATEROLOG_DEEP',
    'LLS': 'RES_LATEROLOG_SHALLOW',
    'LLS_1': 'RES_LATEROLOG_SHALLOW',
    'MSFL': 'RES_MICROSPHERICAL',
    'MSFL_1': 'RES_MICROSPHERICAL',
    'SFLU': 'RES_UNFOCUSED_SHALLOW',
    'SFLU_1': 'RES_UNFOCUSED_SHALLOW',
    'SFLU_2': 'RES_UNFOCUSED_SHALLOW',
    'SFLA': 'RES_FOCUSED_SHALLOW',
    'SFLA_1': 'RES_FOCUSED_SHALLOW',
    'SFLA_2': 'RES_FOCUSED_SHALLOW',
    'SFLT': 'RES_SHALLOW',
    
    # Densidade
    'RHOB': 'DEN',
    'RHOB_1': 'DEN',
    'RHOB_2': 'DEN',
    'RHOT': 'DEN',
    'DRHO': 'DEN_CORR',
    'DRHO_1': 'DEN_CORR',
    'DRHO_2': 'DEN_CORR',
    
    # Porosidade Neutrônica
    'NPHI': 'NEUT',
    'NPHI_1': 'NEUT',
    'NPHI_2': 'NEUT',
    'NEUT': 'NEUT',
    'TNPH': 'NEUT',
    'TNPH_1': 'NEUT',
    'NPOR': 'NEUT',
    'NPOR_1': 'NEUT',
    
    # Sônico
    'DT': 'SONIC',
    'DT_1': 'SONIC',
    'DT_2': 'SONIC',
    'DTSI': 'SLOWNESS',
    'DTSI_1': 'SLOWNESS',
    'DTSI_2': 'SLOWNESS',
    'TTI': 'SONIC_INT',
    'TTI_1': 'SONIC_INT',
    'TTI_2': 'SONIC_INT',
    'TTI_3': 'SONIC_INT',
    'TTI_4': 'SONIC_INT',
    'TTID': 'SONIC_INT',
    
    # Caliper
    'CALI': 'CALI',
    'CALI_1': 'CALI',
    'CALI_2': 'CALI',
    'CALI_3': 'CALI',
    'CALI_4': 'CALI',
    'CAL': 'CALI',
    'CIL': 'CALI',
    'CALS': 'CALI',
    'CAL1': 'CALI',
    'CALT': 'CALI',
    
    # Imageamentos e direcionais
    'DEVI': 'DEVIATION',
    'DEVI_1': 'DEVIATION',
    'AZIM': 'AZIMUTH',
    'HAZI': 'AZIMUTH',
    'AZIE': 'AZIMUTH',
    'DIP': 'DIP',
    'P1AZ': 'AZIMUTH_P1',
    'RB': 'R_BULK',
    'RAD1': 'RADIAL_1',
    'RAD2': 'RADIAL_2',
    'RAD3': 'RADIAL_3',
    'RAD4': 'RADIAL_4',
    
    # Pressão e tensão
    'PESS': 'PRESSURE',
    'TENS': 'TENSION',
    'TENS_1': 'TENSION',
    'TENS_2': 'TENSION',
    'TENS_3': 'TENSION',
    
    # Geoquímica/Espectrometria
    'POTA': 'POTASSIUM',
    'POTA_1': 'POTASSIUM',
    'THOR': 'THORIUM',
    'THOR_1': 'THORIUM',
    'URAN': 'URANIUM',
    'URAN_1': 'URANIUM',
    'W1NG': 'WINDOW_1',
    'W1NG_1': 'WINDOW_1',
    'W2NG': 'WINDOW_2',
    'W2NG_1': 'WINDOW_2',
    'W3NG': 'WINDOW_3',
    'W3NG_1': 'WINDOW_3',
    'W4NG': 'WINDOW_4',
    'W4NG_1': 'WINDOW_4',
    'W5NG': 'WINDOW_5',
    'W5NG_1': 'WINDOW_5',
    
    # Outros (agrupando por similaridade)
    'ITT': 'TEMP_INTAKE',
    'ITT_1': 'TEMP_INTAKE',
    'ITT_2': 'TEMP_INTAKE',
    'TOT': 'TEMP_OUT',
    'TOT_1': 'TEMP_OUT',
    'TOT_2': 'TEMP_OUT',
    'TOT_3': 'TEMP_OUT',
    'TOT_4': 'TEMP_OUT',
    'TOTD': 'TEMP_OUT',
    'SN': 'NEAR_SONIC',
    'SN_1': 'NEAR_SONIC',
    'SN_2': 'NEAR_SONIC',
    'CILD': 'CALI_DUAL',
    'CILD_1': 'CALI_DUAL',
    'CILD_2': 'CALI_DUAL',
    'ILM': 'RES_MEDIUM',
    'ILM_1': 'RES_MEDIUM',
    'ILM_2': 'RES_MEDIUM',
    'LL3': 'RES_LATEROLOG_3',
    'LL3_1': 'RES_LATEROLOG_3',
    'FCNL': 'FOCUSED_CURRENT',
    'FCNL_1': 'FOCUSED_CURRENT',
    'FCNL_2': 'FOCUSED_CURRENT',
    'NCNL': 'NEAR_CURRENT',
    'NCNL_1': 'NEAR_CURRENT',
    'NCNL_2': 'NEAR_CURRENT',
    'AMP': 'AMPLITUDE',
    'AMP_1': 'AMPLITUDE',
    'AMP_2': 'AMPLITUDE',
    'SRAT': 'S_RATIO',
    'SRAT_1': 'S_RATIO',
    'NRAT': 'N_RATIO',
    'NRAT_1': 'N_RATIO',
    'PEF': 'PHOTO_EFFECT',
    'PEF_1': 'PHOTO_EFFECT',
    'HPEF': 'PHOTO_EFFECT_H',
    'HPEF_1': 'PHOTO_EFFECT_H',
    
    # Padronização de sufixos numéricos (_1, _2, etc.)
    **{f'{base}_{n}': base for base in [
        'MINV', 'MNOR', 'MLL', 'C13', 'C24', 'LITH', 'LL', 'LS', 'LSRH', 
        'LU', 'LURH', 'SS1', 'SS2', 'SS1RH', 'DB1', 'DB2', 'DB3', 'DB4',
        'SB1', 'SB2', 'PART', 'CLEX', 'SHRP', 'NP', 'HT12', 'HT23', 'HT34',
        'HT41', 'CDDS', 'DCX', 'NM13', 'NM24', 'OFLG', 'KFLG', 'TIME', 'RGR',
        'RWA', 'CS', 'HSTA', 'REFE', 'RC', 'EMEX', 'PP', 'TEMP', 'FEP1', 'FEP2',
        'RC1', 'RC2', 'FEP', 'CBL', 'FFDC', 'NFDC', 'TT1', 'TT2', 'TT3', 'TT4',
        'DPAP', 'CL', 'NSDR', 'VDEP', 'EWDR', 'RX0', 'RX', 'DDQR', 'CBFS', 'CBLF',
        'SMIN', 'SMNO', 'DTL', 'TSPD', 'T1', 'T2', 'T3', 'T4', 'CNST', 'NPHS',
        'NPHD', 'DPHI', 'PHI', 'P15V', 'M60V', 'P60V', 'VREF', 'SPE', 'FISH',
        'HSP', 'ZB', 'ZM', 'DZB', 'DZM', 'DQZE', 'DQCA', 'MQZE', 'MQCA', 'SPQZ',
        'SPQC', 'PSQ', 'DRCO', 'DXCO', 'CCER', 'MRCO', 'MXCO', 'VRES', 'XFRC',
        'HDRS', 'HMRS', 'DFL', 'XHDR', 'XHMR', 'XDFL', 'HDCN', 'HMCN', 'HDR',
        'HDX', 'HMR', 'HMX', 'STEM', 'GND', 'P5V', 'M15V', 'LAT', 'LN', 'QUAF',
        'S12', 'S23A', 'S34A', 'S4A1', 'S13A', 'DPL1', 'DPL2', 'DB1A', 'DB2A',
        'DB3A', 'DB4A', 'REV', 'GNOR', 'FNOR', 'FINC', 'EV', 'EI', 'ETIM', 'RLU',
        'RLS', 'RLIT', 'RSS1', 'RSS2', 'QLS', 'QSS', 'NRHO', 'TNRA', 'RCFT', 'RCNT',
        'RLUL', 'RLUU', 'RSLL', 'RSLU', 'RSUL', 'RSUU', 'PARI', 'LSHV', 'SSHV', 'FFSS',
        'FFLS', 'SLDT', 'QRLS', 'QRSS', 'DALP', 'RTNR', 'TALP', 'NUCA', 'ENRA', 'RCEF',
        'RCEN', 'ENPH', 'CFTC', 'CNTC', 'CFEC', 'CNEC', 'RCAL', 'SA1', 'SA2', 'SA3',
        'RHGX', 'BS', 'SHVD', 'LHVD', 'RLLL', 'RLLU', 'RHLL', 'RHLU', 'RHLS', 'RHLI',
        'RHS1', 'RHS2', 'HNRH', 'HDAL', 'HPHN', 'HRHO', 'HDRH', 'HDPH', 'HLL', 'HLU',
        'HLS', 'HLIT', 'HSS1', 'HSS2', 'RHFT', 'RHNT', 'HTNP', 'HNPO', 'HCFT', 'HCNT',
        'HTAL', 'IHV', 'ICV', 'HDIR', 'DIPA', 'GRAD', 'DRIF', 'DRAZ', 'RLLD', 'RLLS',
        'CILM', 'CMSF', 'XLL3', 'FREC', 'CNST_1', 'CNST_2', 'TSPD_1', 'TSPD_2', 'TSPD_3',
        'CILM_1', 'CILM_2', 'HDFL', 'AZAP', 'SPHI', 'STSG', 'CTEM', 'CCSW', 'HV', 'RSPA',
        'RSP', 'SPAR', 'SPMV', 'DIFF', 'MARK', 'IICS', 'RSFL', 'RILM', 'TOD', 'SSLT',
        'SVEL', 'RLUL_1', 'RLUU_1', 'RSLL_1', 'RSLU_1', 'RSUL_1', 'RSUU_1', 'PARI_1',
        'LSHV_1', 'SSHV_1', 'FFSS_1', 'FFLS_1', 'SLDT_1', 'QRLS_1', 'QRSS_1', 'DALP_1',
        'RTNR_1', 'TALP_1', 'NUCA_1', 'ENRA_1', 'RCEF_1', 'RCEN_1', 'ENPH_1', 'CFTC_1',
        'CNTC_1', 'CFEC_1', 'CNEC_1', 'RCAL_1', 'SA1_1', 'SA2_1', 'SA3_1', 'RHGX_1',
        'BS_1', 'SHVD_1', 'LHVD_1', 'RLLL_1', 'RLLU_1', 'RHLL_1', 'RHLU_1', 'RHLS_1',
        'RHLI_1', 'RHS1_1', 'RHS2_1', 'HPEF_1', 'HNRH_1', 'HDAL_1', 'HPHN_1', 'HRHO_1',
        'HDRH_1', 'HDPH_1', 'HLL_1', 'HLU_1', 'HLS_1', 'HLIT_1', 'HSS1_1', 'HSS2_1',
        'RHFT_1', 'RHNT_1', 'HTNP_1', 'HNPO_1', 'HCFT_1', 'HCNT_1', 'HTAL_1', 'RHGR_1',
        'HGR_1', 'IHV_1', 'ICV_1', 'ILM_2', 'SFLA_2', 'SPAR_1', 'SPAR_2', 'TIME_1',
        'TIME_2', 'TT1_2', 'CS_2', 'TT2_2', 'TT3_2', 'TT4_2', 'ETIM_2', 'HNPO_1',
        'HTNP_1', 'RHFT_1', 'RHNT_1', 'HDRH_1', 'HNRH_1', 'HRHO_1', 'RHLI_1', 'RHLL_1',
        'RHLS_1', 'RHLU_1', 'RHS1_1', 'RHS2_1', 'AMP_2', 'LLD_1', 'LLS_1', 'MSFL_1',
        'DTSI_1', 'DTSI_2', 'CALI_4', 'DDQR', 'CBFS', 'CBLF', 'SMIN', 'SMNO', 'DTL',
        'CAL1', 'TOT_3', 'TOT_4', 'TTI_3', 'TTI_4', 'SFLT'
    ] for n in range(1, 10)}
}

# =============================================================================
# FUNÇÕES DE PROCESSAMENTO
# =============================================================================

def remove_duplicate_columns(df):
    """Remove colunas duplicadas mantendo a primeira ocorrência"""
    # Verifica colunas duplicadas
    duplicated_cols = df.columns[df.columns.duplicated()]
    if len(duplicated_cols) > 0:
        print(f"Removendo {len(duplicated_cols)} colunas duplicadas: {list(duplicated_cols)}")
        return df.loc[:, ~df.columns.duplicated()]
    return df

def standardize_missing_values(df):
    """
    Substitui todos os -999.25 por NaN e garante consistência nos missing values.
    Também remove linhas completamente vazias (opcional).
    """
    # Substitui -999.25 por NaN
    df.replace(-999.25, pd.NA, inplace=True)
    
    # Opcional: Remove linhas onde todas as colunas (exceto 'ID') são NaN
    cols_to_check = [col for col in df.columns if col != 'ID']
    df.dropna(how='all', subset=cols_to_check, inplace=True)
    
    return df

def process_csv_file(input_path, output_path):
    """Processa um arquivo CSV: padroniza colunas, remove duplicatas, padroniza missing values e adiciona ID"""
    try:
        # Ler o CSV
        df = pd.read_csv(input_path)
        
        # Padronizar nomes das colunas
        new_columns = []
        for col in df.columns:
            clean_col = str(col).strip().upper()
            new_columns.append(CHANNEL_MAPPING.get(clean_col, clean_col))
        df.columns = new_columns
        
        # Remover colunas duplicadas (mantém a primeira ocorrência)
        df = remove_duplicate_columns(df)
        
        # Padronizar missing values (-999.25 → NaN)
        df = standardize_missing_values(df)
        
        # Adicionar coluna ID (nome do arquivo sem extensão)
        filename = os.path.splitext(os.path.basename(input_path))[0]
        df.insert(0, 'ID', filename)
        
        # Salvar o arquivo processado
        df.to_csv(output_path, index=False)
        return True
    
    except Exception as e:
        print(f"Erro ao processar {os.path.basename(input_path)}: {str(e)}")
        return False

def process_all_files(input_dir, output_dir):
    """Processa todos os arquivos CSV no diretório"""
    os.makedirs(output_dir, exist_ok=True)
    csv_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.csv')]
    
    print(f"Encontrados {len(csv_files)} arquivos CSV")
    
    success_count = 0
    for filename in tqdm(csv_files, desc="Processando"):
        input_path = os.path.join(input_dir, filename)
        output_path = os.path.join(output_dir, filename)
        
        if process_csv_file(input_path, output_path):
            success_count += 1
    
    # Relatório final
    print(f"\nProcessamento concluído:")
    print(f"✅ {success_count} arquivos processados com sucesso")
    print(f"❌ {len(csv_files) - success_count} arquivos com erro")

# =============================================================================
# EXECUÇÃO PRINCIPAL
# =============================================================================

if __name__ == "__main__":
    print("==============================================")
    print("  PROCESSAMENTO COMPLETO DE ARQUIVOS CSV")
    print("==============================================")
    print("→ Padroniza nomes de colunas")
    print("→ Remove colunas duplicadas")
    print("→ Adiciona coluna ID\n")
    process_all_files(INPUT_DIR, OUTPUT_DIR)