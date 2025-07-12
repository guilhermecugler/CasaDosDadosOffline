# Autor: Guilherme Cugler https://github.com/guilhermecugler
# Data: 2024-10-29
# Descrição: Interface para consulta de CNPJs no banco SQLite com autocomplete para municípios usando CTkScrollableDropdown

import customtkinter as ctk
import os
import sqlite3
from datetime import datetime
import time
from tkinter import filedialog as fd
from threading import Thread, Event
from PIL import Image
from utils.get_cnae import get_cnaes
from utils.get_cities import get_cities
from utils.get_cnpj_numbers import get_cnpj_numbers_sqlite
from utils.get_cnpj_data import get_cnpj_data_sqlite, get_all_cnpj_data_sqlite
from utils.excel_utils import save_excel
import pandas as pd
from CTkScrollableDropdown import CTkScrollableDropdown

global cancel
cancel = Event()

global progress_step
progress_step = 0

def start_thread(function):
    print(f'Thread {function} started')
    t = Thread(target=function)
    t.daemon = True
    t.start()

def get_municipio_codigo(municipio_name, db_path=os.path.join(os.path.dirname(__file__), "..", "dados-publicos", "cnpj.db")):
    """Retrieve the municipio codigo from the database based on the municipality name."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT codigo FROM municipio WHERE descricao = ?", (municipio_name.upper(),))
        result = cursor.fetchone()
        conn.close()
        if result:
            return result[0]
        print(f"No código found for municipality: {municipio_name}")
        return None
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None

class FiltersFrame(ctk.CTkFrame):
    def __init__(self, master, title):
        super().__init__(master)
        self.grid_columnconfigure(2, weight=1)
        self.title = title
        self.checkboxes = []
        self.cities_cache = {}  # Cache for cities by state

        self.title = ctk.CTkLabel(self, text="Filtros", corner_radius=5)
        self.title.grid(row=0, column=0, padx=10, pady=10, sticky="ew", columnspan=3)

        try:
            cnaes = get_cnaes()
        except Exception as e:
            print(f"Erro ao carregar CNAEs: {e}")
            cnaes = [[], []]

        self.check_somente_mei_var = ctk.BooleanVar(value=False)
        self.check_excluir_mei_var = ctk.BooleanVar(value=False)
        self.check_com_telefone_var = ctk.BooleanVar(value=True)
        self.check_somente_fixo_var = ctk.BooleanVar(value=False)
        self.check_somente_matriz_var = ctk.BooleanVar(value=True)
        self.check_somente_filial_var = ctk.BooleanVar(value=True)
        self.check_somente_celular_var = ctk.BooleanVar(value=True)
        self.check_com_email_var = ctk.BooleanVar(value=True)
        self.check_atividade_secundaria_var = ctk.BooleanVar(value=True)
        self.combobox_estados_var = ctk.StringVar(value='Todos Estados')
        self.combobox_municipios_var = ctk.StringVar(value='Todos Municipios')
        self.combobox_cnae_var = ctk.StringVar(value='Todas Atividades')
        self.cnae_code_var = ctk.StringVar(value='')

        def combobox_estados_callback(choice):
            self.combobox_estados_var.set(choice)
            self.combobox_municipios_var.set('Todos Municipios')
            self.entry_municipios.delete(0, 'end')
            self.entry_municipios.insert(0, '')
            if choice != 'Todos Estados':
                try:
                    if choice not in self.cities_cache:
                        self.cities_cache[choice] = get_cities(choice)
                    self.municipio_dropdown.configure(values=['Todos Municipios'] + self.cities_cache[choice])
                except Exception as e:
                    print(f"Erro ao carregar cidades: {e}")
                    self.municipio_dropdown.configure(values=['Todos Municipios'])
            else:
                self.municipio_dropdown.configure(values=['Todos Municipios'])

        def combobox_cnae_callback(choice):
            self.combobox_cnae_var.set(choice)
            if choice == 'Todas Atividades':
                self.cnae_code_var.set('')
            else:
                for i, cnae_desc in enumerate(cnaes[0]):
                    if cnae_desc == choice:
                        self.cnae_code_var.set(cnaes[1][i])
                        print(f"CNAE selected code: {self.cnae_code_var.get()}")
                        break
                else:
                    self.cnae_code_var.set('')
                    print(f"CNAE not found: {choice}")

        def select_municipio(choice):
            self.combobox_municipios_var.set(choice)
            self.entry_municipios.delete(0, 'end')
            self.entry_municipios.insert(0, choice)

        def format_date_inicial(event):
            text = self.entry_data_inicial.get().replace("/", "")[:8]
            new_text = ""
            if event.keysym.lower() == "backspace":
                return
            for index in range(len(text)):
                if not text[index] in "0123456789":
                    continue
                if index in [1, 3]:
                    new_text += text[index] + "/"
                else:
                    new_text += text[index]
            self.entry_data_inicial.delete(0, "end")
            self.entry_data_inicial.insert(0, new_text)

        def format_date_final(event):
            text = self.entry_data_final.get().replace("/", "")[:8]
            new_text = ""
            if event.keysym.lower() == "backspace":
                return
            for index in range(len(text)):
                if not text[index] in "0123456789":
                    continue
                if index in [1, 3]:
                    new_text += text[index] + "/"
                else:
                    new_text += text[index]
            self.entry_data_final.delete(0, "end")
            self.entry_data_final.insert(0, new_text)

        self.entry_termo = ctk.CTkEntry(self, placeholder_text='Razão Social ou Termo - Ex: Celular')
        self.entry_termo.grid(row=1, column=0, padx=10, pady=10, sticky='ew', columnspan=3)

        self.check_somente_mei = ctk.CTkCheckBox(self, text='Somente MEI', variable=self.check_somente_mei_var, onvalue=True, offvalue=False)
        self.check_somente_mei.grid(row=2, column=0, padx=10, pady=10, sticky='ew')

        self.check_excluir_mei = ctk.CTkCheckBox(self, text='Excluir MEI', variable=self.check_excluir_mei_var, onvalue=True, offvalue=False)
        self.check_excluir_mei.grid(row=2, column=1, padx=10, pady=10, sticky='ew')

        self.check_com_telefone = ctk.CTkCheckBox(self, text='Com Telefone', variable=self.check_com_telefone_var, onvalue=True, offvalue=False)
        self.check_com_telefone.grid(row=2, column=2, padx=10, pady=10, sticky='ew')

        self.check_somente_fixo = ctk.CTkCheckBox(self, text='Somente Fixo', variable=self.check_somente_fixo_var, onvalue=True, offvalue=False)
        self.check_somente_fixo.grid(row=3, column=0, padx=10, pady=10, sticky='ew')

        self.check_somente_matriz = ctk.CTkCheckBox(self, text='Somente Matriz', variable=self.check_somente_matriz_var, onvalue=True, offvalue=False)
        self.check_somente_matriz.grid(row=3, column=1, padx=10, pady=10, sticky='ew')

        self.check_somente_filial = ctk.CTkCheckBox(self, text='Somente Filial', variable=self.check_somente_filial_var, onvalue=True, offvalue=False)
        self.check_somente_filial.grid(row=3, column=2, padx=10, pady=10, sticky='ew')

        self.check_somente_celular = ctk.CTkCheckBox(self, text='Somente Celular', variable=self.check_somente_celular_var, onvalue=True, offvalue=False)
        self.check_somente_celular.grid(row=4, column=0, padx=10, pady=10, sticky='ew')

        self.check_com_email = ctk.CTkCheckBox(self, text='Com E-mail', variable=self.check_com_email_var, onvalue=True, offvalue=False)
        self.check_com_email.grid(row=4, column=1, padx=10, pady=10, sticky='ew')

        self.check_atividade_secundaria = ctk.CTkCheckBox(self, text='Atividade Secundária', variable=self.check_atividade_secundaria_var, onvalue=True, offvalue=False)
        self.check_atividade_secundaria.grid(row=4, column=2, padx=10, pady=10, sticky='ew')

        self.combobox_estados = ctk.CTkComboBox(self, values=['Todos Estados', 'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MS', 'MT', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'], command=combobox_estados_callback, variable=self.combobox_estados_var)
        self.combobox_estados.grid(row=5, column=0, padx=10, pady=10, sticky='ew')

        self.entry_municipios = ctk.CTkEntry(self, placeholder_text='Digite o município', textvariable=self.combobox_municipios_var)
        self.entry_municipios.grid(row=5, column=1, padx=10, pady=10, sticky='ew')
        self.municipio_dropdown = CTkScrollableDropdown(attach=self.entry_municipios, values=['Todos Municipios'], autocomplete=True, height=200, command=select_municipio)

        self.entry_bairro = ctk.CTkEntry(self, placeholder_text='Bairro')
        self.entry_bairro.grid(row=5, column=2, padx=10, pady=10, sticky='ew')

        self.entry_CEP = ctk.CTkEntry(self, placeholder_text='CEP')
        self.entry_CEP.grid(row=6, column=0, padx=10, pady=10, sticky='ew')

        self.entry_DDD = ctk.CTkEntry(self, placeholder_text='DDD')
        self.entry_DDD.grid(row=6, column=1, padx=10, pady=10, sticky='ew')

        self.combobox_cnaes = ctk.CTkComboBox(self, values=['Todas Atividades'] + cnaes[0], command=combobox_cnae_callback, variable=self.combobox_cnae_var)
        self.combobox_cnaes.grid(row=6, column=2, padx=10, pady=10, sticky='ew')

        self.label_data = ctk.CTkLabel(self, text="Período de abertura")
        self.label_data.grid(row=7, column=0, padx=10, pady=10, sticky='ew')

        self.entry_data_inicial = ctk.CTkEntry(self, placeholder_text='Inicio - 01/01/2023')
        self.entry_data_inicial.bind("<KeyRelease>", command=format_date_inicial)
        self.entry_data_inicial.grid(row=7, column=1, padx=10, pady=10, sticky='ew')

        self.entry_data_final = ctk.CTkEntry(self, placeholder_text='Fim - 01/12/2023')
        self.entry_data_final.bind("<KeyRelease>", command=format_date_final)
        self.entry_data_final.grid(row=7, column=2, padx=10, pady=10, sticky='ew')

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        if cancel.is_set():
            self.status_update("Cancelado com sucesso!")
            return

        self.width = 550
        self.height = 650
        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        x = int((screen_width / 2) - (self.width / 2))
        y = int((screen_height / 2) - (self.height / 2))

        self.title("Casa dos Dados")
        self.geometry(f'{self.width}x{self.height}+{x}+{y-30}')
        self.grid_columnconfigure((0, 1), weight=1)
        self.grid_rowconfigure(2, weight=1)
        self.resizable(width=False, height=False)

        assets = os.path.join(os.path.dirname(__file__), "..", "assets")
        self.iconbitmap(os.path.join(assets, "icon.ico"))

        self.home_image = ctk.CTkImage(light_image=Image.open(os.path.join(assets, "logo_casa_dos_dados_light.png")),
                                       dark_image=Image.open(os.path.join(assets, "logo_casa_dos_dados_dark.png")), size=(500, 56))
        self.home_image_label = ctk.CTkLabel(self, text="", image=self.home_image)
        self.home_image_label.grid(row=0, column=0, padx=10, pady=(30, 10), sticky="ew", columnspan=4)

        self.filters_frame = FiltersFrame(self, "Filtros")
        self.filters_frame.grid(row=1, column=0, padx=10, pady=(10, 0), sticky="nsew", columnspan=4)

        self.label_max_cnpjs = ctk.CTkLabel(self, text="Máximo de CNPJs")
        self.label_max_cnpjs.grid(row=5, column=0, padx=(20, 0), pady=10, sticky="w")

        self.entry_max_cnpjs_var = ctk.IntVar()
        self.entry_max_cnpjs_var.set(1000)
        self.entry_max_cnpjs = ctk.CTkEntry(self, placeholder_text="1000", width=100, textvariable=self.entry_max_cnpjs_var)
        self.entry_max_cnpjs.grid(row=5, column=0, padx=(120, 0), pady=10, sticky="w")

        self.button_buscar_empresas = ctk.CTkButton(self, text="Buscar Empresas", command=self.button_buscar_empresas_callback)
        self.button_buscar_empresas.grid(row=5, column=1, padx=10, pady=10, sticky="ew", columnspan=2)

        self.button_cancelar = ctk.CTkButton(self, text="Cancelar", command=self.button_cancelar_callback, state="disabled")
        self.button_cancelar.grid(row=5, column=3, padx=10, pady=10, sticky="ew")

        self.status = ctk.CTkLabel(self, text="Faça uma busca!")
        self.status.grid(row=6, column=0, padx=0, pady=0, sticky="ew", columnspan=4)

        self.progress_bar = ctk.CTkProgressBar(self, orientation='horizontal')
        self.progress_bar.grid(row=7, column=0, padx=10, pady=(5, 5), sticky="ew", columnspan=4)
        self.progress_bar.set(0)

        self.label_file_type = ctk.CTkLabel(self, text="Tipo de arquivo:")
        self.label_file_type.grid(row=8, column=1)

        self.file_type_var = ctk.StringVar(value='xlsx')
        self.radio_xlsx = ctk.CTkRadioButton(self, text='Planilha', value='xlsx', variable=self.file_type_var, command=self.radiobutton_event, radiobutton_width=13, radiobutton_height=13)
        self.radio_xlsx.grid(row=8, column=2, padx=0, pady=0, sticky="w")

        self.radio_csv = ctk.CTkRadioButton(self, text='CSV', value='csv', variable=self.file_type_var, command=self.radiobutton_event, radiobutton_width=13, radiobutton_height=13)
        self.radio_csv.grid(row=8, column=3, padx=0, pady=0, sticky="e")

        self.file_entry_var = ctk.Variable(value=f"{datetime.strftime(datetime.now(), '%d-%m-%Y %H-%M')}.{self.file_type_var.get()}")
        self.file_entry = ctk.CTkEntry(self, textvariable=self.file_entry_var)
        self.file_entry.grid(row=9, column=1, padx=10, pady=20, sticky="ew")

        self.button_select_folder = ctk.CTkButton(self, text="Selecionar Pasta", command=self.button_select_folder_callback)
        self.button_select_folder.grid(row=9, column=2, padx=10, pady=10, sticky="ew", columnspan=2)

        self.appearance_mode_menu = ctk.CTkOptionMenu(self, values=["Sistema", "Escuro", "Claro"], command=self.change_appearance_mode_event)
        self.appearance_mode_menu.grid(row=9, column=0, padx=10, pady=20, sticky="ws")

    def get_save_folder(self):
        directory = fd.askdirectory(title='Selecionar pasta')
        return directory

    def button_select_folder_callback(self):
        directory = self.get_save_folder()
        if directory == '':
            return
        file_location = f"{directory}/{datetime.strftime(datetime.now(), '%d-%m-%Y %H-%M')}.{self.file_type_var.get()}"
        self.file_entry_var.set(file_location.replace('//', '/'))

    def radiobutton_event(self):
        self.file_entry_var.set(f"{datetime.strftime(datetime.now(), '%d-%m-%Y %H-%M')}.{self.file_type_var.get()}")

    def progress_bar_update(self, step):
        self.progress_bar.set(step)
        self.update_idletasks()

    def status_update(self, text):
        self.status.configure(text=text)
        self.update_idletasks()

    def button_cancelar_callback(self):
        cancel.set()
        self.button_cancelar.configure(state='disabled')
        self.status_update("Cancelando, aguarde...")
        if cancel.is_set():
            self.button_buscar_empresas.configure(state='normal')
            return

    def button_buscar_empresas_callback(self):
        self.button_buscar_empresas.configure(state='disabled')
        self.button_cancelar.configure(state='normal')
        cancel.clear()

        json_filters = {}
        try:
            max_cnpjs = self.entry_max_cnpjs_var.get()
            if max_cnpjs < 1:
                max_cnpjs = None

            # Get municipio codigo instead of name
            municipio_name = self.filters_frame.combobox_municipios_var.get()
            municipio_codigo = None
            if municipio_name != 'Todos Municipios':
                municipio_codigo = get_municipio_codigo(municipio_name)
                if not municipio_codigo:
                    self.status_update(f"Erro: Município '{municipio_name}' não encontrado no banco de dados.")
                    self.button_buscar_empresas.configure(state='normal')
                    self.button_cancelar.configure(state='disabled')
                    return

            json_filters.update(
                {
                    'query': {
                        'termo': [] if not self.filters_frame.entry_termo.get() else [self.filters_frame.entry_termo.get()],
                        'atividade_principal': [] if not self.filters_frame.cnae_code_var.get() else [self.filters_frame.cnae_code_var.get()],
                        'natureza_juridica': [],
                        'uf': [] if self.filters_frame.combobox_estados_var.get() == 'Todos Estados' else [self.filters_frame.combobox_estados_var.get()],
                        'municipio': [] if municipio_codigo is None else [str(municipio_codigo)],  # Use codigo instead of name
                        'cep': [] if not self.filters_frame.entry_CEP.get() else [self.filters_frame.entry_CEP.get()],
                        'ddd': [] if not self.filters_frame.entry_DDD.get() else [self.filters_frame.entry_DDD.get()],
                        'bairro': [] if not self.filters_frame.entry_bairro.get() else [self.filters_frame.entry_bairro.get()],
                    },
                    'range_query': {
                        'data_abertura': {
                            'lte': None if not self.filters_frame.entry_data_final.get() else datetime.strftime(datetime.strptime(self.filters_frame.entry_data_final.get(), '%d/%m/%Y'), '%Y-%m-%d'),
                            'gte': None if not self.filters_frame.entry_data_inicial.get() else datetime.strftime(datetime.strptime(self.filters_frame.entry_data_inicial.get(), '%d/%m/%Y'), '%Y-%m-%d'),
                        }
                    },
                    'extras': {
                        'somente_mei': self.filters_frame.check_somente_mei_var.get(),
                        'excluir_mei': self.filters_frame.check_excluir_mei_var.get(),
                        'com_email': self.filters_frame.check_com_email_var.get(),
                        'incluir_atividade_secundaria': self.filters_frame.check_atividade_secundaria_var.get(),
                        'com_contato_telefonico': self.filters_frame.check_com_telefone_var.get(),
                        'somente_fixo': self.filters_frame.check_somente_fixo_var.get(),
                        'somente_celular': self.filters_frame.check_somente_celular_var.get(),
                        'somente_matriz': self.filters_frame.check_somente_matriz_var.get(),
                        'somente_filial': self.filters_frame.check_somente_filial_var.get()
                    },
                    'max_cnpjs': max_cnpjs,
                    'page': 1
                })

        except ValueError as e:
            print(f'Error: {e}')
            self.status_update(f"Erro nos filtros: {e}")
            self.button_buscar_empresas.configure(state='normal')
            self.button_cancelar.configure(state='disabled')
            return

        self.progress_bar.set(0)

        def buscar():
            start_time = time.time()
            self.progress_bar.configure(mode="indeterminate")
            self.progress_bar.start()
            self.status_update("Buscando dados...")

            try:
                data = get_all_cnpj_data_sqlite(json_filters, self.status_update)
            except Exception as e:
                self.status_update(f"Erro ao buscar dados: {e}")
                self.button_buscar_empresas.configure(state='normal')
                self.button_cancelar.configure(state='disabled')
                return

            self.progress_bar.stop()
            self.progress_bar.configure(mode="determinate")

            if cancel.is_set():
                self.status_update("Cancelado com sucesso!")
                self.button_buscar_empresas.configure(state='normal')
                self.button_cancelar.configure(state='disabled')
                return

            max_cnpjs = json_filters.get('max_cnpjs')
            if max_cnpjs is not None:
                data = data[:max_cnpjs]
            self.status_update(f"Salvando {len(data)} registro(s)...")
            file_name = self.file_entry_var.get()
            df = pd.DataFrame(data)
            if not df.empty:
                if file_name.endswith('.xlsx'):
                    save_excel(df, file_name)
                else:
                    df.to_csv(file_name, index=False, encoding='utf-8')
            quantidade_salvo = len(df)

            self.button_buscar_empresas.configure(state='normal')
            self.button_cancelar.configure(state='disabled')

            tempo = "%s segundos" % int((time.time() - start_time))
            self.progress_bar.set(1)
            self.status_update(f"Finalizado... salvos {quantidade_salvo} CNPJ(s) em {tempo}")

        start_thread(buscar)

    def change_appearance_mode_event(self, new_appearance_mode):
        if new_appearance_mode == "Escuro":
            new_appearance_mode = "Dark"
        elif new_appearance_mode == "Claro":
            new_appearance_mode = "Light"
        elif new_appearance_mode == "Sistema":
            new_appearance_mode = "System"
        ctk.set_appearance_mode(new_appearance_mode)

if __name__ == "__main__":
    app = App()
    app.mainloop()