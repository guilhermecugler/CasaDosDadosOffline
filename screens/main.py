# Autor: Guilherme Cugler https://github.com/guilhermecugler
# Data: 2024-10-29
# Descrição: Interface para consulta de CNPJs no banco SQLite com autocomplete para municípios usando CTkScrollableDropdown

import customtkinter as ctk
import os
import sqlite3
from datetime import datetime
import time
from tkinter import filedialog as fd
from tkinter import messagebox
from threading import Thread, Event
from PIL import Image
from utils.get_cnae import get_cnaes
from utils.get_cities import get_cities
from utils.get_cnpj_numbers import get_cnpj_numbers_sqlite
from utils.get_cnpj_data import get_cnpj_data_sqlite, get_all_cnpj_data_sqlite
from utils.excel_utils import save_excel
import pandas as pd
from CTkScrollableDropdown import CTkScrollableDropdown
from utils.database_updater import update_cnpj_database, DatabaseUpdateError, UpdateCancelled

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
        self.check_somente_matriz_var = ctk.BooleanVar(value=False)
        self.check_somente_filial_var = ctk.BooleanVar(value=False)
        self.check_somente_celular_var = ctk.BooleanVar(value=False)
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

        self.width = 780
        self.height = 720
        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        x = int((screen_width / 2) - (self.width / 2))
        y = int((screen_height / 2) - (self.height / 2))

        self.title("Casa dos Dados")
        self.geometry(f'{self.width}x{self.height}+{x}+{y - 30}')
        for column in range(4):
            self.grid_columnconfigure(column, weight=1)
        self.grid_rowconfigure(1, weight=1)

        assets = os.path.join(os.path.dirname(__file__), "..", "assets")
        self.iconbitmap(os.path.join(assets, "icon.ico"))
        self.update_cancel_event = Event()
        self._update_in_progress = False

        header_frame = ctk.CTkFrame(self, fg_color="transparent")
        header_frame.grid(row=0, column=0, padx=10, pady=(20, 10), sticky="ew", columnspan=4)
        header_frame.grid_columnconfigure(0, weight=1)

        self.home_image = ctk.CTkImage(
            light_image=Image.open(os.path.join(assets, "logo_casa_dos_dados_light.png")),
            dark_image=Image.open(os.path.join(assets, "logo_casa_dos_dados_dark.png")),
            size=(500, 56),
        )
        self.home_image_label = ctk.CTkLabel(header_frame, text="", image=self.home_image)
        self.home_image_label.grid(row=0, column=0, sticky="w")

        self.button_update_database = ctk.CTkButton(
            header_frame,
            text="Atualizar Base Offline",
            width=200,
            command=self.button_update_database_callback,
        )
        self.button_update_database.grid(row=0, column=1, padx=(10, 0), sticky="e")

        self.filters_frame = FiltersFrame(self, "Filtros")
        self.filters_frame.grid(row=1, column=0, padx=10, pady=(10, 0), sticky="nsew", columnspan=4)

        controls_frame = ctk.CTkFrame(self, fg_color="transparent")
        controls_frame.grid(row=2, column=0, padx=10, pady=(10, 10), sticky="ew", columnspan=4)
        controls_frame.grid_columnconfigure(0, weight=0)
        controls_frame.grid_columnconfigure(1, weight=0)
        controls_frame.grid_columnconfigure(2, weight=1)
        controls_frame.grid_columnconfigure(3, weight=0)

        self.label_max_cnpjs = ctk.CTkLabel(controls_frame, text="Maximo de CNPJs")
        self.label_max_cnpjs.grid(row=0, column=0, padx=(0, 10), pady=5, sticky="w")

        self.entry_max_cnpjs_var = ctk.IntVar(value=1000)
        self.entry_max_cnpjs = ctk.CTkEntry(controls_frame, width=120, textvariable=self.entry_max_cnpjs_var)
        self.entry_max_cnpjs.grid(row=0, column=1, padx=(0, 25), pady=5, sticky="w")

        self.button_buscar_empresas = ctk.CTkButton(
            controls_frame, text="Buscar Empresas", command=self.button_buscar_empresas_callback
        )
        self.button_buscar_empresas.grid(row=0, column=2, padx=(0, 10), pady=5, sticky="ew")

        self.button_cancelar = ctk.CTkButton(
            controls_frame, text="Cancelar", command=self.button_cancelar_callback, state="disabled"
        )
        self.button_cancelar.grid(row=0, column=3, padx=(0, 0), pady=5, sticky="ew")

        self.status = ctk.CTkLabel(self, text="Faca uma busca!")
        self.status.grid(row=3, column=0, padx=10, pady=(0, 0), sticky="ew", columnspan=4)

        self.progress_bar = ctk.CTkProgressBar(self, orientation='horizontal')
        self.progress_bar.grid(row=4, column=0, padx=10, pady=(5, 5), sticky="ew", columnspan=4)
        self.progress_bar.set(0)

        export_frame = ctk.CTkFrame(self, fg_color="transparent")
        export_frame.grid(row=5, column=0, padx=10, pady=(10, 20), sticky="ew", columnspan=4)
        export_frame.grid_columnconfigure(0, weight=0)
        export_frame.grid_columnconfigure(1, weight=1)
        export_frame.grid_columnconfigure(2, weight=0)

        self.appearance_mode_menu = ctk.CTkOptionMenu(
            export_frame, values=["Sistema", "Escuro", "Claro"], command=self.change_appearance_mode_event
        )
        self.appearance_mode_menu.grid(row=0, column=0, padx=(0, 20), pady=5, sticky="w")

        file_type_frame = ctk.CTkFrame(export_frame, fg_color="transparent")
        file_type_frame.grid(row=0, column=1, pady=5, sticky="w")
        file_type_frame.grid_columnconfigure(0, weight=0)
        file_type_frame.grid_columnconfigure(1, weight=0)
        file_type_frame.grid_columnconfigure(2, weight=0)

        self.label_file_type = ctk.CTkLabel(file_type_frame, text="Tipo de arquivo:")
        self.label_file_type.grid(row=0, column=0, padx=(0, 10), sticky="w")

        self.file_type_var = ctk.StringVar(value='xlsx')
        self.radio_xlsx = ctk.CTkRadioButton(
            file_type_frame, text='Planilha', value='xlsx', variable=self.file_type_var, command=self.radiobutton_event,
            radiobutton_width=13, radiobutton_height=13
        )
        self.radio_xlsx.grid(row=0, column=1, padx=(0, 10), sticky="w")

        self.radio_csv = ctk.CTkRadioButton(
            file_type_frame, text='CSV', value='csv', variable=self.file_type_var, command=self.radiobutton_event,
            radiobutton_width=13, radiobutton_height=13
        )
        self.radio_csv.grid(row=0, column=2, sticky="w")

        self.file_entry_var = ctk.Variable(value=f"{datetime.strftime(datetime.now(), '%d-%m-%Y %H-%M')}.{self.file_type_var.get()}")
        self.file_entry = ctk.CTkEntry(export_frame, textvariable=self.file_entry_var)
        self.file_entry.grid(row=1, column=0, columnspan=2, padx=(0, 10), pady=(0, 0), sticky="ew")

        self.button_select_folder = ctk.CTkButton(
            export_frame, text="Selecionar Pasta", command=self.button_select_folder_callback
        )
        self.button_select_folder.grid(row=1, column=2, padx=(10, 0), pady=(0, 0), sticky="ew")

        self.progress_bar.set(0)


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

    def button_update_database_callback(self):
        if self._update_in_progress:
            return
        if self.button_buscar_empresas.cget("state") == "disabled":
            messagebox.showinfo(
                "Atualizar banco offline",
                "Finalize a consulta atual antes de iniciar a atualizacao do banco offline.",
            )
            return

        confirm = messagebox.askyesno(
            "Atualizar banco offline",
            (
                "Este processo baixa aproximadamente 60 GB de dados da Receita Federal e pode levar varias horas. "
                "As pastas 'dados-publicos' e 'dados-publicos-zip' serao limpas antes do download. Deseja continuar?"
            ),
        )
        if not confirm:
            return

        self._update_in_progress = True
        self.update_cancel_event.clear()
        self.button_update_database.configure(state="disabled")
        self.button_buscar_empresas.configure(state="disabled")
        self.button_cancelar.configure(state="disabled")
        cancel.clear()
        self.progress_bar.stop()
        self.progress_bar.configure(mode="determinate")
        self.progress_bar.set(0)
        self.status_update("Preparando atualizacao do banco offline...")
        start_thread(self._run_database_update)

    def _status_from_thread(self, message: str) -> None:
        self.after(0, lambda: self.status_update(message))

    def _progress_from_thread(self, value: float) -> None:
        def _update():
            self.progress_bar.configure(mode="determinate")
            self.progress_bar.set(max(0.0, min(value, 1.0)))

        self.after(0, _update)

    def _search_progress_from_thread(self, value: float) -> None:
        def _update():
            self.progress_bar.configure(mode="determinate")
            self.progress_bar.set(max(0.0, min(value, 1.0)))

        self.after(0, _update)

    def _restore_search_buttons(self) -> None:
        def _update():
            self.button_buscar_empresas.configure(state='normal')
            self.button_update_database.configure(state='normal')
            self.button_cancelar.configure(state='disabled')

        self.after(0, _update)

    def _finalize_update_ui(self) -> None:
        self.button_update_database.configure(state="normal")
        self.button_buscar_empresas.configure(state="normal")
        self.button_cancelar.configure(state="disabled")
        self._update_in_progress = False

    def _run_database_update(self) -> None:
        try:
            update_cnpj_database(
                status_callback=self._status_from_thread,
                progress_callback=self._progress_from_thread,
                cancel_event=self.update_cancel_event,
            )
        except UpdateCancelled:
            self._status_from_thread("Atualizacao cancelada.")
            self._progress_from_thread(0.0)
        except DatabaseUpdateError as exc:
            self._status_from_thread(f"Erro na atualizacao: {exc}")
            self._progress_from_thread(0.0)
        except Exception as exc:
            self._status_from_thread(f"Erro inesperado: {exc}")
            self._progress_from_thread(0.0)
        finally:
            self.after(0, self._finalize_update_ui)


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

    def button_buscar_empresas_callback(self):
        self.button_buscar_empresas.configure(state='disabled')
        self.button_update_database.configure(state='disabled')
        self.button_cancelar.configure(state='normal')
        self.progress_bar.configure(mode="determinate")
        self.progress_bar.set(0)
        cancel.clear()

        limit_text = ''
        try:
            limit_text = self.entry_max_cnpjs.get().strip()
        except AttributeError:  # fallback para uso do IntVar em versões mais antigas
            limit_text = str(self.entry_max_cnpjs_var.get()).strip()

        max_cnpjs = None
        if limit_text:
            try:
                value = int(limit_text)
                if value > 0:
                    max_cnpjs = value
            except ValueError:
                self.status_update("Informe um numero valido para o limite de CNPJs.")
                self._restore_search_buttons()
                return

        filtros = self.filters_frame
        somente_mei = filtros.check_somente_mei_var.get()
        excluir_mei = filtros.check_excluir_mei_var.get()
        if somente_mei and excluir_mei:
            self.status_update("Escolha apenas uma opcao entre 'Somente MEI' e 'Excluir MEI'.")
            self._restore_search_buttons()
            return

        somente_fixo = filtros.check_somente_fixo_var.get()
        somente_celular = filtros.check_somente_celular_var.get()
        if somente_fixo and somente_celular:
            self.status_update("Escolha apenas uma opcao de telefone (Fixo ou Celular).")
            self._restore_search_buttons()
            return

        somente_matriz = filtros.check_somente_matriz_var.get()
        somente_filial = filtros.check_somente_filial_var.get()
        if somente_matriz and somente_filial:
            self.status_update("Escolha apenas Matriz ou Filial.")
            self._restore_search_buttons()
            return

        com_contato = filtros.check_com_telefone_var.get() or somente_fixo or somente_celular

        municipio_name = filtros.combobox_municipios_var.get()
        municipio_codigo = None
        if municipio_name != 'Todos Municipios':
            municipio_codigo = get_municipio_codigo(municipio_name)
            if not municipio_codigo:
                self.status_update(f"Erro: Municipio '{municipio_name}' nao encontrado no banco de dados.")
                self._restore_search_buttons()
                return

        try:
            data_inicial_input = filtros.entry_data_inicial.get().strip()
            data_final_input = filtros.entry_data_final.get().strip()
            data_inicial_fmt = datetime.strftime(datetime.strptime(data_inicial_input, '%d/%m/%Y'), '%Y-%m-%d') if data_inicial_input else None
            data_final_fmt = datetime.strftime(datetime.strptime(data_final_input, '%d/%m/%Y'), '%Y-%m-%d') if data_final_input else None
        except ValueError as exc:
            self.status_update(f"Erro nos filtros: {exc}")
            self._restore_search_buttons()
            return

        termo = filtros.entry_termo.get().strip()
        cep_raw = filtros.entry_CEP.get().strip()
        cep_digits = ''.join(ch for ch in cep_raw if ch.isdigit())
        ddd_text = filtros.entry_DDD.get().strip()
        bairro_text = filtros.entry_bairro.get().strip()

        extras = {
            'somente_mei': somente_mei,
            'excluir_mei': excluir_mei,
            'com_email': filtros.check_com_email_var.get(),
            'incluir_atividade_secundaria': filtros.check_atividade_secundaria_var.get(),
            'com_contato_telefonico': com_contato,
            'somente_fixo': somente_fixo,
            'somente_celular': somente_celular,
            'somente_matriz': somente_matriz,
            'somente_filial': somente_filial,
        }

        json_filters = {
            'query': {
                'termo': [termo] if termo else [],
                'atividade_principal': [filtros.cnae_code_var.get()] if filtros.cnae_code_var.get() else [],
                'natureza_juridica': [],
                'uf': [] if filtros.combobox_estados_var.get() == 'Todos Estados' else [filtros.combobox_estados_var.get()],
                'municipio': [] if municipio_codigo is None else [str(municipio_codigo)],
                'cep': [cep_digits] if cep_digits else [],
                'ddd': [ddd_text] if ddd_text else [],
                'bairro': [bairro_text] if bairro_text else [],
            },
            'range_query': {
                'data_abertura': {
                    'lte': data_final_fmt,
                    'gte': data_inicial_fmt,
                }
            },
            'extras': extras,
            'max_cnpjs': max_cnpjs,
        }

        self.status_update("Buscando dados no banco local...")
        self._search_progress_from_thread(0.02)

        def buscar():
            start_time = time.time()
            try:
                self._search_progress_from_thread(0.05)
                data = get_all_cnpj_data_sqlite(
                    json_filters,
                    self.status_update,
                    progress_callback=self._search_progress_from_thread,
                    cancel_event=cancel,
                    limit_hint=max_cnpjs,
                )

                if cancel.is_set():
                    self.status_update("Cancelado com sucesso!")
                    self._search_progress_from_thread(0.0)
                    return

                registros = len(data)
                if max_cnpjs is not None and registros > max_cnpjs:
                    data = data[:max_cnpjs]
                    registros = len(data)

                self.status_update(f"Encontrados {registros} registro(s).")

                file_name = self.file_entry_var.get()
                if data:
                    df = pd.DataFrame(data)
                    self._search_progress_from_thread(0.9)
                    try:
                        if file_name.endswith('.xlsx'):
                            save_excel(df, file_name)
                        else:
                            df.to_csv(file_name, index=False, encoding='utf-8')
                        elapsed = int(time.time() - start_time)
                        self._search_progress_from_thread(1.0)
                        self.status_update(f"Finalizado... salvos {registros} CNPJ(s) em {elapsed} segundos")
                    except Exception as exc:
                        self._search_progress_from_thread(1.0)
                        self.status_update(f"Erro ao salvar arquivo: {exc}")
                else:
                    self._search_progress_from_thread(1.0)
                    self.status_update("Nenhum dado para salvar")
            except Exception as exc:
                self._search_progress_from_thread(0.0)
                self.status_update(f"Erro ao buscar dados: {exc}")
            finally:
                cancel.clear()
                self._restore_search_buttons()

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
