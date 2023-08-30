#-------------------------------------------------------------------------------
# Name:        Short current
# Purpose:
#
# Author:      Trong duc
#
# Created:     01/08/2023
# Copyright:   (c) Trong 2023
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import sqlite3
from tabulate import tabulate
import cmath

class SC:
    def __init__(self, dbfile):
        # Kết nối đến cơ sở dữ liệu SQLite
        self.dbfile = dbfile
        self.conn = sqlite3.connect(self.dbfile)
        self.cursor = self.conn.cursor()
    def create_unique_constraint(self):
        try:
            self.cursor.execute("ALTER TABLE BUS_RESULT ADD CONSTRAINT unique_no UNIQUE (NO)")
            self.cursor.execute("ALTER TABLE BRANCH_RESULT ADD CONSTRAINT unique_no UNIQUE (NO)")
            self.conn.commit()
        except sqlite3.OperationalError as e:
            # Đã có ràng buộc UNIQUE trên cột "NO", không cần thêm lại
            pass

    def _array2dict(self, dict_keys, dict_values):
        return dict(zip(dict_keys, dict_values))


    def _fetch_table_data(self, table_name):
        self.cursor.execute(f'SELECT * FROM {table_name}')
        columns = [column[0] for column in self.cursor.description]
        table_data = {column: [] for column in columns}
        rows = self.cursor.fetchall()
        for row in rows:
            for i, value in enumerate(row):
                table_data[columns[i]].append(value)
        return table_data

    def get_bus_data(self):
        return self._fetch_table_data('BUS')

    def get_line_data(self):
        return self._fetch_table_data('LINE')

    def get_load_data(self):
        return self._fetch_table_data('LOAD')

    def get_gen_data(self):
        return self._fetch_table_data('GEN')

    def get_x2_data(self):
        return self._fetch_table_data('X2TRANSFORMER')

    def get_shunt_data(self):
        return self._fetch_table_data('SHUNT')

    def get_option(self):
        return self._fetch_table_data('OPTION')
    def save_result(self):
        BUS_RESULT = 'BUS_RESULT'
        BRANCH_RESULT = 'BRANCH_RESULT'

        # Duyệt qua từ điển và thêm từng cặp khóa-giá trị vào bảng 'BUS_RESULT' trong cơ sở dữ liệu
        for key, value in self.v_dict.items():
            rounded_value = round(value.real, 4)

            # Kiểm tra xem khóa đã tồn tại trong bảng hay chưa
            self.cursor.execute(f"SELECT * FROM {BUS_RESULT} WHERE NO = ?", (key,))
            existing_data = self.cursor.fetchone()

            if existing_data:
                # Khóa đã tồn tại, tiến hành UPDATE
                self.cursor.execute(f"UPDATE {BUS_RESULT} SET MAG = ? WHERE NO = ?", (str(rounded_value), key))
            else:
                # Khóa chưa tồn tại, tiến hành INSERT
                self.cursor.execute(f"INSERT INTO {BUS_RESULT} (NO, MAG) VALUES (?, ?)", (key, str(rounded_value)))

        for key, value in self.vdelta_dict.items():
            rounded_value = round(value.real * 180 / cmath.pi, 4)

            # Kiểm tra xem khóa đã tồn tại trong bảng hay chưa
            self.cursor.execute(f"SELECT * FROM {BUS_RESULT} WHERE NO = ?", (key,))
            existing_data = self.cursor.fetchone()

            if existing_data:
                # Khóa đã tồn tại, tiến hành UPDATE
                self.cursor.execute(f"UPDATE {BUS_RESULT} SET ANG = ? WHERE NO = ?", (str(rounded_value), key))
            else:
                # Khóa chưa tồn tại, tiến hành INSERT
                self.cursor.execute(f"INSERT INTO {BUS_RESULT} (NO, ANG) VALUES (?, ?)", (key, str(rounded_value)))

        for key, value in self.frombus.items():
            rounded_pbr = round(self.sbr_dict[key].real * self.sbase, 4)
            rounded_qbr = round(self.sbr_dict[key].imag * self.sbase, 4)
            rounded_pbr_ = round(self.sbr__dict[key].real * self.sbase, 4)
            rounded_qbr_ = round(self.sbr__dict[key].imag * self.sbase, 4)
            rounded_ibr = round(abs(self.ibr_dict[key]), 4)
            rounded_ibr_ = round(abs(self.ibr__dict[key]), 4)

            # Kiểm tra xem khóa đã tồn tại trong bảng hay chưa
            self.cursor.execute(f"SELECT * FROM {BRANCH_RESULT} WHERE NO = ?", (key,))
            existing_data = self.cursor.fetchone()

            if existing_data:
                # Khóa đã tồn tại, tiến hành UPDATE
                self.cursor.execute(f"UPDATE {BRANCH_RESULT} SET FROMBUS = ? WHERE NO = ?", (value, key))
                self.cursor.execute(f"UPDATE {BRANCH_RESULT} SET TOBUS = ? WHERE NO = ?", (self.tobus[key], key))
                self.cursor.execute(f"UPDATE {BRANCH_RESULT} SET CID = ? WHERE NO = ?", (self.cid[key], key))
                self.cursor.execute(f"UPDATE {BRANCH_RESULT} SET P1 = ? WHERE NO = ?", (rounded_pbr, key))
                self.cursor.execute(f"UPDATE {BRANCH_RESULT} SET Q1 = ? WHERE NO = ?", (rounded_qbr, key))
                self.cursor.execute(f"UPDATE {BRANCH_RESULT} SET P2 = ? WHERE NO = ?", (rounded_pbr_, key))
                self.cursor.execute(f"UPDATE {BRANCH_RESULT} SET Q2 = ? WHERE NO = ?", (rounded_qbr_, key))
                self.cursor.execute(f"UPDATE {BRANCH_RESULT} SET I1 = ? WHERE NO = ?", (rounded_ibr, key))
                self.cursor.execute(f"UPDATE {BRANCH_RESULT} SET I2 = ? WHERE NO = ?", (rounded_ibr_, key))
            else:
                # Khóa chưa tồn tại, tiến hành INSERT
                self.cursor.execute(f"INSERT INTO {BRANCH_RESULT} (NO, FROMBUS, TOBUS, P1, Q1, P2, Q2, I1, I2) "
                                    f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                    (key, value, self.tobus[key], rounded_pbr, rounded_qbr, rounded_pbr_,
                                     rounded_qbr_, rounded_ibr, rounded_ibr_))

        # Lưu các thay đổi vào cơ sở dữ liệu
        self.conn.commit()
        branch = []
        for key, value in self.frombus.items():
            row = []
            rounded_pbr = round(self.sbr_dict[key].real * self.sbase, 4)
            rounded_qbr = round(self.sbr_dict[key].imag * self.sbase, 4)
            rounded_pbr_ = round(self.sbr__dict[key].real * self.sbase, 4)
            rounded_qbr_ = round(self.sbr__dict[key].imag * self.sbase, 4)
            rounded_ibr = round(abs(self.ibr_dict[key]), 4)
            rounded_ibr_ = round(abs(self.ibr__dict[key]), 4)
            row.append(self.tobus[key])
            row.append(rounded_pbr)
            row.append(rounded_qbr)
            row.append(rounded_ibr)
            branch.append(row)
        branch_dict = self._array2dict(self.frombus, branch)

        return self.h_dict, self.v_dict, self.vdelta_dict, branch_dict

    def close_connection(self):
        # Đóng kết nối
        self.conn.close()

    def ybus(self):
        self.bus_data = self.get_bus_data()
        self.line_data = self.get_line_data()
        x2_data = self.get_x2_data()
        self.gen_data = self.get_gen_data()
        self.load_data = self.get_load_data()
        shunt_data = self.get_shunt_data()

        n = len(self.bus_data['NO'])
        self.ybus = [[0] * n for _ in range(n)]

        def add_ybus_element(frombus, tobus, y, bq):
            self.ybus[frombus - 1][tobus - 1] -= y
            self.ybus[frombus - 1][frombus - 1] += y  - complex(0, bq)
            self.ybus[tobus - 1][tobus - 1] += y
            self.ybus[tobus - 1][frombus - 1] -= y

        # Calculate Ybus Matrix for LINE table
        for i in range(len(self.line_data['NO'])):
            frombus = self.line_data['FROMBUS'][i]
            tobus = self.line_data['TOBUS'][i]
            x = float(self.line_data['Xpu'][i])
            y = 1 / complex(0, x)
            add_ybus_element(frombus, tobus, y, 0)

        # Calculate Ybus Matrix for X2TRANSFORMER table
        for i in range(len(x2_data['NO'])):
            frombus = x2_data['FROMBUS'][i]
            tobus = x2_data['TOBUS'][i]
            x = float(x2_data['Xpu'][i])
            y = 1 / complex(0, x)
            add_ybus_element(frombus, tobus, y, 0)

        # Calculate Ybus Matrix for SHUNT table
        for i in range(len(shunt_data['NO'])):
            frombus = shunt_data['BUS'][i]
            bq = (-shunt_data['Q_nom'][i] * (500 ** 2)) / (100 * (shunt_data['U_nom'][i] ** 2))
##            print(frombus, bq)
            add_ybus_element(frombus, frombus, 0, bq)

        # Directly print the modulus and phase lists of the Ybus matrix
        self.ybus_module = [[round(abs(element), 4) for element in row] for row in self.ybus]
        self.ybus_phase = [[round(cmath.phase(element), 4) for element in row] for row in self.ybus]

##        print("ybus:\n", tabulate(self.ybus, tablefmt="grid"))
        for i in range(len(shunt_data['NO'])+1):
            frombus = self.gen_data['NO'][i]
            x = float(self.gen_data['Xd'][i])
            y = 1 / complex(0, x)
            self.ybus[frombus - 1][frombus - 1] += y

##        print("ybus:\n", tabulate(self.ybus, tablefmt="grid"))
    def zbus(self):
        n = len(self.ybus)
        X = [[0] * (2*n) for _ in range(n)]

        # Augment A with identity matrix I to form [A | I]
        for i in range(n):
            for j in range(n):
                X[i][j] = self.ybus[i][j]
                if i == j:
                    X[i][j+n] = 1

        # Perform Gaussian elimination
        for i in range(n):
            # Find the pivot row
            pivot_row = i
            for j in range(i+1, n):
                if abs(X[j][i]) > abs(X[pivot_row][i]):
                    pivot_row = j

            # Swap pivot row with current row
            X[i], X[pivot_row] = X[pivot_row], X[i]

            # Scale the pivot row to make the pivot element 1
            pivot = X[i][i]
            for j in range(i, 2*n):
                X[i][j] /= pivot

            # Perform row operations to make other elements in the column zero
            for j in range(n):
                if j != i:
                    factor = X[j][i]
                    for k in range(i, 2*n):
                        X[j][k] -= factor * X[i][k]

        # Extract the inverted matrix from [A | I]
        self.zbus = [row[n:] for row in X]

##        print("Nghịch đảo của A:")
##        for row in self.zbus:
##            print(row)

    def cal_sc(self):
        v_prefault = [((complex(1.05,0))) for _ in range(len(self.zbus))]
        i_prefault = []
        n = [1,2,3,4,5]
        for i in n:
            i_prefault.append([i,v_prefault[i-1]/self.zbus[i-1][i-1]])
##        print('v_prefault',v_prefault)
##        print('i_prefault',i_prefault)
        for row in i_prefault:
            print('%25d'%row[0],'%24.4f'%abs(row[1]))
if __name__ == '__main__':
    dbfile = '5nut.db'
    sc = SC(dbfile)
    sc.ybus()
    sc.zbus()
    sc.cal_sc()