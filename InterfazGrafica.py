from tkinter import Tk, Button, Label, Frame, filedialog
from tkcalendar import DateEntry
from INEvalidador import Validador

validador = Validador()

def on_button_click():
    fecha_inicio = date_entry1.get_date()
    fecha_fin = date_entry2.get_date()
    validador.validar_encuesta(fecha_inicio, fecha_fin)

def load_file():
    filepath = filedialog.askopenfilename(initialdir="/", title="Seleccione archivo",
                                          filetypes=(("Archivos de texto", "*.txt"), ("todos los archivos", "*.*")))
    if filepath:
        with open(filepath, 'r') as file:
            content = file.read()
        
        # Aquí puedes modificar el contenido del archivo como lo necesites
        modified_content = content.upper()  # ejemplo: convertir todo a mayúsculas
        
        with open(filepath, 'w') as file:
            file.write(modified_content)

root = Tk()
root.title("Validador ENCOVI")

frame = Frame(root)
frame.pack(pady=10)

# Etiquetas y campos de entrada de fecha
Label(frame, text="Fecha de inicio:").grid(row=0, column=0)
date_entry1 = DateEntry(frame, width=12, background='darkblue',
                        foreground='white', borderwidth=2)
date_entry1.grid(row=0, column=1)

Label(frame, text="Fecha de fin:").grid(row=1, column=0)
date_entry2 = DateEntry(frame, width=12, background='darkblue',
                        foreground='white', borderwidth=2)
date_entry2.grid(row=1, column=1)

button_validate = Button(root, text="Validar Encuesta", command=on_button_click)
button_validate.pack()

button_load = Button(root, text="Cargar Archivo", command=load_file)
button_load.pack()

root.mainloop()
