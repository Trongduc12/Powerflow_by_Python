#-------------------------------------------------------------------------------
# Name:        GUI
# Purpose:
#
# Author:      Trong duc
#
# Created:     07/08/2023
# Copyright:   (c) Trong 2023
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox
import sqlite3
from tabulate import tabulate
import cmath

# Import NR, GS class từ file mã nguồn hiện có
from Newton_Raphson import NR
from Gauss import Gauss

class PowerFlowGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Giao diện Power Flow")
        self.create_widgets()

    def create_widgets(self):
        # Nhãn hướng dẫn
        self.label1 = tk.Label(self.root, text="Chọn tệp CSDL SQLite:")
        self.label1.pack()

        # Nút để chọn tệp cơ sở dữ liệu SQLite
        self.select_db_button = tk.Button(self.root, text="Chọn CSDL", command=self.open_database)
        self.select_db_button.pack()

        # Nhãn hiển thị tên tệp đã chọn
        self.selected_db_label = tk.Label(self.root, text="")
        self.selected_db_label.pack()

        # OptionMenu để chọn phương pháp tính toán
        def update_op_idx(*args):
            self.op_idx = self.calculation_method_var.get()
            print(self.op_idx)
        self.calculation_method_var = tk.StringVar(self.root)  # Biến lưu trữ giá trị được chọn
        self.calculation_method_var.set("Chọn độ chính xác")  # Thiết lập giá trị mặc định
        self.calculation_method_options = ["OP1", "OP2"]  # Danh sách các tùy chọn
        self.calculation_method_menu = tk.OptionMenu(self.root, self.calculation_method_var, *self.calculation_method_options)
        self.calculation_method_var.trace("w", update_op_idx)  # Thêm callback để theo dõi giá trị khi thay đổi
        self.op_idx = self.calculation_method_var.get()
        self.calculation_method_menu.pack()
        # Nút để chạy tính toán Power Flow bằng phương pháp Newton-Raphson
        self.run_nr_button = tk.Button(self.root, text="NEWTON-RAPHSON", command=self.run_NR)
        self.run_nr_button.pack()

        # Nút để chạy tính toán Power Flow bằng phương pháp Newton-Raphson1
        self.run_nr_button = tk.Button(self.root, text="NEWTON-RAPHSON(Quick Calculation)", command=self.run_NR1)
        self.run_nr_button.pack()


        # Nút để chạy tính toán Power Flow bằng phương pháp Gauss-Seidel
        self.run_gs_button = tk.Button(self.root, text="GAUSS-SEIDEL", command=self.run_GS)
        self.run_gs_button.pack()

        # Nút để chạy tính toán Power Flow bằng phương pháp Gauss-Seidel1
        self.run_gs_button = tk.Button(self.root, text="GAUSS-SEIDEL(Quick Calculation)", command=self.run_GS1)
        self.run_gs_button.pack()

        # Vùng hiển thị kết quả
        self.result_text = tk.Text(self.root, width=150, height=50)
        self.result_text.pack()

    def open_database(self):
        # Viết mã để mở một tệp cơ sở dữ liệu SQLite
        file_path = filedialog.askopenfilename(filetypes=[("SQLite files", "*.db")])
        if file_path:
            self.selected_db_label.config(text="Tệp đã chọn: " + file_path)
            self.db_file_path = file_path  # Lưu đường dẫn của tệp cơ sở dữ liệu cho sử dụng sau này

    def run_NR(self):
        # Tạo một thể hiện của lớp NR và chuyển tệp cơ sở dữ liệu cho lớp NR
        if hasattr(self, 'db_file_path'):
            nr_instance = NR(self.db_file_path, self.op_idx)
            nr_instance.ybus()
            nr_instance.checkbus()
            results = nr_instance.save_result()
            # print("Intermediate results:", results)
            self.display_results(results)
        # Hiển thị thông báo thành công
            messagebox.showinfo("Thành công", "Phương pháp Newton-Raphson đã chạy thành công!")
        else:
            messagebox.showwarning("Cảnh báo", "Vui lòng chọn tệp cơ sở dữ liệu trước khi chạy Newton-Raphson.")

    def run_NR1(self):
        # Tạo một thể hiện của lớp NR và chuyển tệp cơ sở dữ liệu cho lớp NR
        if hasattr(self, 'db_file_path'):
            nr_instance = NR(self.db_file_path, self.op_idx)
            nr_instance.ybus()
            nr_instance.cal_nr1()
            results = nr_instance.save_result()
            # print("Intermediate results:", results)
            self.display_results(results)
        # Hiển thị thông báo thành công
            messagebox.showinfo("Thành công", "Phương pháp Newton-Raphson đã chạy thành công!")
        else:
            messagebox.showwarning("Cảnh báo", "Vui lòng chọn tệp cơ sở dữ liệu trước khi chạy Newton-Raphson.")


    def run_GS(self):
        if hasattr(self, 'db_file_path'):
            g = Gauss(self.db_file_path, self.op_idx)
            g.ybus()
            g.cal_gaus()
            results = g.save_result()
            # print("Intermediate results:", results)
            self.display_results(results)
        # Hiển thị thông báo thành công
            messagebox.showinfo("Thành công", "Phương pháp GAUSS-SEIDEL đã chạy thành công!")
        else:
            messagebox.showwarning("Cảnh báo", "Vui lòng chọn tệp cơ sở dữ liệu trước khi chạy GAUSS-SEIDEL.")

    def run_GS1(self):
        if hasattr(self, 'db_file_path'):
            g = Gauss(self.db_file_path, self.op_idx)
            g.ybus()
            g.cal_gaus1()
            results = g.save_result()
            # print("Intermediate results:", results)
            self.display_results(results)
        # Hiển thị thông báo thành công
            messagebox.showinfo("Thành công", "Phương pháp GAUSS-SEIDEL đã chạy thành công!")
        else:
            messagebox.showwarning("Cảnh báo", "Vui lòng chọn tệp cơ sở dữ liệu trước khi chạy GAUSS-SEIDEL.")

    # def display_results(self, results):
    #     # Hiển thị kết quả tính toán Power Flow trong vùng hiển thị kết quả
    #     self.result_text.delete(1.0, tk.END)
    #     self.result_text.insert(tk.END, tabulate(results, headers="keys", tablefmt="grid"))
    def display_results(self, result_list):
        # Hiển thị kết quả tính toán Power Flow trong vùng hiển thị kết quả
        self.result_text.delete(1.0, tk.END)

        # Tạo một chuỗi để chứa thông tin từ các từ điển
        result_str = ""

        # Duyệt qua từng từ điển trong danh sách các kết quả

        for idx, results in enumerate(result_list, start=1):
            # if idx == 1:
            #     result_str += f"Tổng số vòng lặp:\n"

            if idx == 2:
                result_str += f"MAG:\n"
            if idx == 3:
                result_str += f"ANG:\n"
            if idx == 4:
                result_str += f"BRANCH:\n"
            # print(results)
            for key, value in results.items():
                result_str += f"{key}: {value}\n"
            result_str += "\n"

        # Hiển thị chuỗi kết quả trong vùng hiển thị kết quả
        self.result_text.insert(tk.END, result_str)



def main():
    root = tk.Tk()
    app = PowerFlowGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
