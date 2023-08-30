#-------------------------------------------------------------------------------
# Name:        Newton_Raphson
# Purpose:
#
# Author:      Trong duc
#
# Created:     07/08/2023
# Copyright:   (c) Trong 2023
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import sqlite3
from tabulate import tabulate
import cmath

class NR:
    def __init__(self, dbfile, op_idx):
        # Kết nối đến cơ sở dữ liệu SQLite
        self.op = None
        self.op_idx = op_idx
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
            self.ybus[frombus - 1][frombus - 1] += y + complex(0, b / 2) - complex(0, bq)
            self.ybus[tobus - 1][tobus - 1] += y + complex(0, b / 2)
            self.ybus[tobus - 1][frombus - 1] -= y

        # Calculate Ybus Matrix for LINE table
        for i in range(len(self.line_data['NO'])):
            frombus = self.line_data['FROMBUS'][i]
            tobus = self.line_data['TOBUS'][i]
            r = float(self.line_data['Rpu'][i])
            x = float(self.line_data['Xpu'][i])
            b = float(self.line_data['Bpu'][i])
            # if self.line_data['CID'][i] == 2:
            #     r = r / 2
            #     x = x / 2
            #     b = 2 * b
            y = 1 / complex(r, x)
            add_ybus_element(frombus, tobus, y, 0)

        # Calculate Ybus Matrix for X2TRANSFORMER table
        for i in range(len(x2_data['NO'])):
            frombus = x2_data['FROMBUS'][i]
            tobus = x2_data['TOBUS'][i]
            r = float(x2_data['Rpu'][i])
            x = float(x2_data['Xpu'][i])
            b = float(x2_data['Bpu'][i])
            y = 1 / complex(r, x)
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

##        print("Modulus List:\n", tabulate(self.ybus_module, tablefmt="grid"))
##        print("Phase List:\n", tabulate(self.ybus_phase, tablefmt="grid"))


    def jacobi(self):
        n = len(self.bus_data['NO'])
        self.j1 = []
        self.j2 = []
        self.j3 = []
        self.j4 = []
        for i in range(n):
            row1 = [0]*n
            self.j1.append(row1)
            row2 = [0]*n
            self.j2.append(row2)
        for i in range(n):
            row3 = [0]*n
            self.j3.append(row3)
        for i in range(n):
            row4 = [0]*n
            self.j4.append(row4)
        for k in range(len(self.bus_data['NO'])):
            if self.codebus[k+1] == 3:
                    continue
            for n in range(len(self.bus_data['NO'])):
                skip = False
                if n != k:
                    if self.codebus[n+1] == 3:
                        skip = True
                    if not skip:
                        self.j1[k][n] = self.v[k]*self.ybus_module[k][n]*self.v[n]*cmath.sin(self.v_delta[k]-self.v_delta[n]-self.ybus_phase[k][n])
                        self.j3[k][n] = -self.v[k]*self.ybus_module[k][n]*self.v[n]*cmath.cos(self.v_delta[k]-self.v_delta[n]-self.ybus_phase[k][n])
                        self.j2[k][n] = self.v[k]*self.ybus_module[k][n]*cmath.cos(self.v_delta[k]-self.v_delta[n]-self.ybus_phase[k][n])
                        self.j4[k][n] = self.v[k]*self.ybus_module[k][n]*cmath.sin(self.v_delta[k]-self.v_delta[n]-self.ybus_phase[k][n])
                    if self.codebus[n+1] == 2:
                        skip = True
                    if not skip:
                        self.j2[k][k] += self.ybus_module[k][n]*self.v[n]*cmath.cos(self.v_delta[k]-self.v_delta[n]-self.ybus_phase[k][n])
                        self.j4[k][k] += self.ybus_module[k][n]*self.v[n]*cmath.sin(self.v_delta[k]-self.v_delta[n]-self.ybus_phase[k][n])
                    self.j3[k][k] += self.v[k]*self.ybus_module[k][n]*self.v[n]*cmath.cos(self.v_delta[k]-self.v_delta[n]-self.ybus_phase[k][n])
                    self.j1[k][k] -= self.v[k]*self.ybus_module[k][n]*self.v[n]*cmath.sin(self.v_delta[k]-self.v_delta[n]-self.ybus_phase[k][n])
                elif n == k:
                    m = self.v[k]*self.ybus_module[k][k]*cmath.cos(self.ybus_phase[k][k])
                    h = -self.v[k]*self.ybus_module[k][k]*cmath.sin(self.ybus_phase[k][k])
            self.j2[k][k] += 2*m
            self.j4[k][k] += 2*h
        for k in range(len(self.bus_data['NO']) - 1, -1, -1):
            for n in range(len(self.bus_data['NO']) - 1, -1, -1):
                if self.codebus[n+1] == 3:
                    self.j1[k].pop(n)
                    self.j2[k].pop(n)
                    self.j3[k].pop(n)
                    self.j4[k].pop(n)
                if self.codebus[n+1] == 2:
                    self.j2[k].pop(n)
                    self.j4[k].pop(n)
            if self.codebus[k+1] == 3:
                self.j1.pop(k)
                self.j2.pop(k)
                self.j3.pop(k)
                self.j4.pop(k)
            if self.codebus[k+1] == 2:
                self.j3.pop(k)
                self.j4.pop(k)

        # print("j1:\n",tabulate(self.j1, tablefmt="grid"))
        # print("j2:\n",tabulate(self.j2, tablefmt="grid"))
        # print("j3:\n",tabulate(self.j3, tablefmt="grid"))
        # print("j4:\n",tabulate(self.j4, tablefmt="grid"))


    def inv_jacobi(self):
        self.j = []
        for i in range(len(self.j1)):
            self.j1[i].extend(self.j2[i])
        for i in range(len(self.j3)):
            self.j3[i].extend(self.j4[i])
        self.j.extend(self.j1)
        self.j.extend(self.j3)
        # print("ma tran jacobi:\n", tabulate(self.j, tablefmt="grid"))
        n = len(self.j)
        X = [[0] * (2*n) for _ in range(n)]

    # Augment A with identity matrix I to form [A | I]
        for i in range(n):
            for j in range(n):
                X[i][j] = self.j[i][j]
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
        self.inv_j = [row[n:] for row in X]

        # print("Nghịch đảo của A:", tabulate(self.inv_j, tablefmt='grid'))
    def cal_nr(self):
        self.x = []
        # print(self.y)
        self.x = [0]*len(self.inv_j)
        for i in range(len(self.inv_j)):
            for j in range(len(self.inv_j[i])):
                self.x[i] += self.inv_j[i][j]*self.y[j]
        for i in range(len(self.codebus)):
            # print(self.codebus[i+1])
            if self.codebus[i+1] == 3:
                self.x.insert(i, 0)
                self.x.insert(i+len(self.codebus), 0)
            if self.codebus[i+1] == 2:
                self.x.insert(i+len(self.codebus), 0)
        for i in range(len(self.x)):
            if i < len(self.v_delta):
                self.v_delta[i] += self.x[i]
            else:
                self.v[i-len(self.v_delta)] += self.x[i]
        self.v_dict = self._array2dict(self.bus_data['NO'], self.v)
        self.vdelta_dict = self._array2dict(self.bus_data['NO'], self.v_delta)
        self.tobus = self._array2dict(self.line_data['NO'], self.line_data['TOBUS'])
        self.frombus = self._array2dict(self.line_data['NO'], self.line_data['FROMBUS'])
        self.cid = self._array2dict(self.line_data['NO'], self.line_data['CID'])
        sbr = []
        sbr_ = []
        ibr = []
        ibr_ = []
        for key, value in self.frombus.items():
            v = cmath.rect(self.v_dict[value].real, self.vdelta_dict[value].real)
            v_ = cmath.rect(self.v_dict[self.tobus[key]].real, self.vdelta_dict[self.tobus[key]].real)
            i = (v - v_)*self.ybus[value-1][self.tobus[key]-1]
            # print('i', i)
            i_ = -i
            s = v*i.conjugate()
            s_ = v_*i_.conjugate()
            # print(s_)
            sbr.append(s)
            sbr_.append(s_)
            ibr.append(i)
            ibr_.append(i_)
        self.sbr_dict = self._array2dict(self.line_data['NO'], sbr)
        self.sbr__dict = self._array2dict(self.line_data['NO'], sbr_)
        self.ibr_dict = self._array2dict(self.line_data['NO'], ibr )
        self.ibr__dict = self._array2dict(self.line_data['NO'], ibr_)
        # print(self.sbr_dict)

    def ch_option(self):
        option = self.get_option()
        for i in range(len(option)):
            if option['NAMEOPT'][i] == self.op_idx:
                self.op = option['VALUE'][i]
        return self.op

    def iteration(self):
        accur = self.ch_option()
        self.sbase = 100
        p = [0]*len(self.bus_data['NO'])
        dp = [0]*len(self.bus_data['NO'])
        q = [0]*len(self.bus_data['NO'])
        dq = [0]*len(self.bus_data['NO'])
        # print("ybus_module:\n", tabulate(self.ybus_module, tablefmt = 'grid'))
        # print("ybus_phase\n", tabulate(self.ybus_phase, tablefmt = 'grid'))
        for k in range(len(self.bus_data['NO']) - 1, -1, -1):
            if self.codebus[k+1] == 3:
                dp.pop(k)
                dq.pop(k)
            if self.codebus[k+1] == 2:
                dq.pop(k)
        self.y = []
        self.y.extend(dp)
        self.y.extend(dq)
        h = 0
        v_memo = []
        while True:
            pass
            h  += 1
            # print(h, self.codebus)
            self.jacobi()
            self.inv_jacobi()
            # self.cal_nr()
            self.p = [0]*len(self.bus_data['NO'])
            dp = [0]*len(self.bus_data['NO'])
            self.q = [0]*len(self.bus_data['NO'])
            dq = [0]*len(self.bus_data['NO'])
            for k in range(len(self.bus_data['NO'])):
                for n in range(len(self.bus_data['NO'])):
                    self.p[k] += self.v[k]*self.ybus_module[k][n]*self.v[n]*cmath.cos(self.v_delta[k]-self.v_delta[n]-self.ybus_phase[k][n])
                    self.q[k] += self.v[k]*self.ybus_module[k][n]*self.v[n]*cmath.sin(self.v_delta[k]-self.v_delta[n]-self.ybus_phase[k][n])

                dp[k] = ((self.pgen.get(k+1, 0) or 0) - (self.pload.get(k+1, 0)) or 0) / self.sbase - self.p[k]
                dq[k] = ((self.qgen.get(k+1, 0) or 0) - (self.qload.get(k+1, 0)) or 0) / self.sbase - self.q[k]
            # print(dq)
            for k in range(len(self.bus_data['NO']) - 1, -1, -1):
                if self.codebus[k+1] == 3:
                    dp.pop(k)
                    dq.pop(k)
                if self.codebus[k+1] == 2:
                    dq.pop(k)
            self.y = []
            self.y.extend(dp)
            self.y.extend(dq)
            v_memo.append(self.v)
            self.cal_nr()
            if max(abs(val) for val in dp) < accur and max(abs(val) for val in dq) < accur:
                break
            self.h_dict = dict([('Số bước lặp:', h)])
            # print('điện áp\n',tabulate(self.v, tablefmt = 'grid'))

            # print(q)
    def checkbus(self):
        self.v =[0] * len(self.bus_data['NO'])
        self.v_delta = [0] * len(self.bus_data['NO'])
        self.v =[0] * len(self.bus_data['NO'])
        self.v_delta = [0] * len(self.bus_data['NO'])
        self.codebus = self._array2dict(self.bus_data['NO'], self.bus_data['Code'])
        self.vSched = self._array2dict(self.gen_data['NO'], self.gen_data['Vsched'])
        self.pgen = self._array2dict(self.gen_data['NO'], self.gen_data['P'])
        self.qgen = self._array2dict(self.gen_data['NO'], self.gen_data['Q'])
        self.qmax = self._array2dict(self.gen_data['NO'], self.gen_data['Qmax'])
        self.qmin = self._array2dict(self.gen_data['NO'], self.gen_data['Qmin'])
        self.pload = self._array2dict(self.load_data['NO'], self.load_data['P'])
        self.qload = self._array2dict(self.load_data['NO'], self.load_data['Q'])
        for key, value in self.codebus.items():
            if value == 3:
                self.v[key -1] = self.vSched[key]
            elif value == 1:
                self.v[key -1] = 1.0
            elif value == 2:
                self.v[key -1] = self.vSched[key]
        self.iteration()
        for k in range(len(self.bus_data['NO'])):
            if self.codebus[k+1] == 2:
                if (self.q[k]).real + (self.qload.get(k+1, 0) or 0) / self.sbase < self.qmin[k+1] / self.sbase:
                    self.qgen[k+1] = self.qmin[k+1]
                    self.codebus[k+1] = 1
                    self.iteration()
                if (self.q[k]).real + (self.qload.get(k+1, 0) or 0) / self.sbase > self.qmax[k+1] / self.sbase:
                    self.qgen[k+1] = self.qmax[k+1]
                    self.codebus[k+1] = 1
                    self.iteration()
        for i in range(len(self.v)):
            self.v[i] = round(self.v[i].real, 4)
        # print('Điện áp:\n', tabulate(self.v, headers="keys", tablefmt="grid"))
    def cal_nr1(self):
        accur = self.ch_option()
        bus_result = self._fetch_table_data('BUS_RESULT')
        self.sbase = 100
        p = [0]*len(self.bus_data['NO'])
        dp = [0]*len(self.bus_data['NO'])
        # dpm = [0]*len(self.bus_data['NO'])
        q = [0]*len(self.bus_data['NO'])
        dq = [0]*len(self.bus_data['NO'])
        # dqm = [0]*len(self.bus_data['NO'])
        self.v =[0] * len(self.bus_data['NO'])
        self.v_delta = [0] * len(self.bus_data['NO'])
        self.codebus = self._array2dict(self.bus_data['NO'], self.bus_data['Code'])
        self.vSched = self._array2dict(self.gen_data['NO'], self.gen_data['Vsched'])
        self.pgen = self._array2dict(self.gen_data['NO'], self.gen_data['P'])
        self.qgen = self._array2dict(self.gen_data['NO'], self.gen_data['Q'])
        self.qmax = self._array2dict(self.gen_data['NO'], self.gen_data['Qmax'])
        self.qmin = self._array2dict(self.gen_data['NO'], self.gen_data['Qmin'])
        self.pload = self._array2dict(self.load_data['NO'], self.load_data['P'])
        self.qload = self._array2dict(self.load_data['NO'], self.load_data['Q'])
        for i in range(len(bus_result['NO'])):
            self.v[i] = float(bus_result['MAG'][i])
            self.v_delta[i] = float(bus_result['ANG'][i])*cmath.pi/180
        self.iteration()
        for k in range(len(self.bus_data['NO'])):
            if self.codebus[k+1] == 2:
                # print()
                if (self.q[k] + (self.qload.get(k+1, 0) or 0) / self.sbase).real < self.qmin[k+1] / self.sbase:
                    self.qgen[k+1] = self.qmin[k+1]
                    self.codebus[k+1] = 1
                    self.iteration()
                if (self.q[k] + (self.qload.get(k+1, 0) or 0) / self.sbase).real > self.qmax[k+1] / self.sbase:
                    self.qgen[k+1] = self.qmax[k+1]
                    self.codebus[k+1] = 1
                    self.iteration()
        for i in range(len(self.v)):
            self.v[i] = round(self.v[i].real, 4)

if __name__ == '__main__':
    dbfile = '5nut.db'
    op_idx = 'OP1'
    nr = NR(dbfile, op_idx)
    nr.ybus()
    nr.checkbus()
    #nr.cal_nrl()
    nr.save_result()
    #nr.print_result()
    nr.close_connection()