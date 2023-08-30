#-------------------------------------------------------------------------------
# Name:        Guass-Siden
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

class Gauss:

    def __init__(self, dbfile, op_idx):
        # Kết nối đến cơ sở dữ liệu SQLite
        self.op = None
        self.dbfile = dbfile
        self.op_idx = op_idx
        self.conn = sqlite3.connect(self.dbfile)
        self.cursor = self.conn.cursor()


    '''Hàm tạo ràng buộc duy nhất cho cột NO trong DB'''
    def create_unique_constraint(self):
        try:
            self.cursor.execute("ALTER TABLE BUS_RESULT ADD CONSTRAINT unique_no UNIQUE (NO)")
            self.cursor.execute("ALTER TABLE BRANCH_RESULT ADD CONSTRAINT unique_no UNIQUE (NO)")
            self.conn.commit()
        except sqlite3.OperationalError as e:
            # Đã có ràng buộc UNIQUE trên cột "NO",
            pass


    '''Hàm chuyển đổi từ kiểu dữ liệu list sang dictionary'''
    def _array2dict(self, dict_keys, dict_values):
        return dict(zip(dict_keys, dict_values))


    '''Hàm lấy dữ liệu bảng từ DB'''
    def _fetch_table_data(self, table_name):
        self.cursor.execute(f'SELECT * FROM {table_name}')
        columns = [column[0] for column in self.cursor.description]
        table_data = {column: [] for column in columns}
        rows = self.cursor.fetchall()
        for row in rows:
            for i, value in enumerate(row):
                table_data[columns[i]].append(value)
        return table_data


    '''Hàm lấy dữ liệu từ 1 bảng xác định bằng hàm _fetch_table_data'''
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


    '''Hàm lưu kết quả tính toán LF vào DB'''
    def save_result(self):
        BUS_RESULT = 'BUS_RESULT'
        BRANCH_RESULT = 'BRANCH_RESULT'

        # Duyệt qua từ điển và thêm từng cặp khóa-giá trị vào bảng 'BUS_RESULT' trong cơ sở dữ liệu
        for key, value in self.v_dict.items():
            rounded_value = round(value.real, 4)

            # Kiểm tra xem NO đã tồn tại trong bảng hay chưa
            self.cursor.execute(f"SELECT * FROM {BUS_RESULT} WHERE NO = ?", (key,))
            existing_data = self.cursor.fetchone()

            if existing_data:
                # NO đã tồn tại, tiến hành UPDATE
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
                # NO chưa tồn tại, tiến hành INSERT
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
        # print(type(branch_dict))
        # print(branch_dict)

        return self.k_dict, self.v_dict, self.vdelta_dict, branch_dict


    '''Đóng kết nối với DB'''
    def close_connection(self):
        # Đóng kết nối
        self.conn.close()

    '''Tính toán Ybus'''
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
            # print(self.ybus[2][3])

        # Calculate Ybus Matrix for LINE table
        for i in range(len(self.line_data['NO'])):
            frombus = self.line_data['FROMBUS'][i]
            tobus = self.line_data['TOBUS'][i]
            r = float(self.line_data['Rpu'][i])
            x = float(self.line_data['Xpu'][i])
            b = float(self.line_data['Bpu'][i])
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
            # print(frombus, tobus, y)
            add_ybus_element(frombus, tobus, y, 0)

        # Calculate Ybus Matrix for SHUNT table
        for i in range(len(shunt_data['NO'])):
            frombus = shunt_data['BUS'][i]
            # kháng
            bq = (-shunt_data['Q_nom'][i] * (500 ** 2)) / (100 * (shunt_data['U_nom'][i] ** 2))
            add_ybus_element(frombus, tobus, 0, bq)

        # Directly print the modulus and phase lists of the Ybus matrix
        self.ybus_module = [[round(abs(element), 4) for element in row] for row in self.ybus]
        self.ybus_phase = [[round(cmath.phase(element), 4) for element in row] for row in self.ybus]

##        print("Modulus List:\n", tabulate(self.ybus_module, tablefmt="grid"))
##        print("Phase List:\n", tabulate(self.ybus_phase, tablefmt="grid"))
##        print(self.ybus)

        # Optional: Print the ybus matrix


    '''Tính LF bằng GS'''
    def gaus(self):
        n = len(self.bus_data['NO'])
        vn = self.v[:]
        for i in range(n):
            # print(self.codebus[5])
            if self.codebus[i+1]==3: #slack bus
                vn[i]=self.v[i]
            if self.codebus[i+1]==1: #PQ bus
                vi = self.s[i].conjugate()/vn[i].conjugate()
                for j in range(n):
                    if i!=j:
                        vi -= self.ybus[i][j]*vn[j]
                vn[i] = 1/self.ybus[i][i]*vi
            elif self.codebus[i+1] == 2: #PV bus
                si = 0
                # print(self.v[i])
                for k in range(n):
                    si += vn[i]*self.ybus[i][k].conjugate()*vn[k].conjugate()
                self.q[i] = si.imag
                if self.q[i] > self.qmin[i+1] / self.sbase and self.q[i] < self.qmax[i+1] / self.sbase:
                    self.s[i] = complex(self.p[i], self.q[i])
                    vi = self.s[i].conjugate()/vn[i].conjugate()
                    for j in range(n):
                        if i!=j:
                            vi -= self.ybus[i][j]*vn[j]
                    vn[i] = 1/self.ybus[i][i]*vi
                    modul, phase = cmath.polar(vn[i])
                    modul = self.vSched[i+1]
                    vn[i] = cmath.rect(modul, phase)
                # print( self.q[i],self.qmin[i+1] / self.sbase)
                if self.q[i] + ((self.qload.get(i+1, 0)) or 0) / self.sbase <= self.qmin[i+1] / self.sbase:
                    print('duoi gioi han nen thay bang Q min')
                    self.qgen[i+1] = self.qmin[i+1]
                    self.q[i] = ((self.qgen.get(i+1, 0) or 0) - (self.qload.get(i+1, 0)) or 0) / self.sbase
                    self.s[i] = complex(self.p[i], self.q[i])
                    vi = self.s[i].conjugate()/vn[i].conjugate()
                    for j in range(n):
                        if i!=j:
                            vi -= self.ybus[i][j]*vn[j]
                    vn[i] = 1/self.ybus[i][i]*vi
                    self.codebus[i+1] = 1
                # print(self.q[i] + ((self.qload.get(i+1, 0)) or 0) / self.sbase, self.qmax[i+1] / self.sbase)
                if self.q[i] + ((self.qload.get(i+1, 0)) or 0) / self.sbase >= self.qmax[i+1] / self.sbase:
                    print('qua gioi han nen thay bang Q max')
                    self.codebus[i+1] = 1
                    self.qgen[i+1] = self.qmax[i+1]

                    self.q[i] = ((self.qgen.get(i+1, 0) or 0) - (self.qload.get(i+1, 0)) or 0) / self.sbase
                    self.s[i] = complex(self.p[i], self.q[i])
                    vi = self.s[i].conjugate()/vn[i].conjugate()
                    for j in range(n):
                        if i!=j:
                            vi -= self.ybus[i][j]*vn[j]
                    vn[i] = 1/self.ybus[i][i]*vi
        return vn
        # print(vn)

    def ch_option(self):
        option = self.get_option()
        for i in range(len(option)):
            if option['NAMEOPT'][i] == self.op_idx:
                self.op = option['VALUE'][i]
        return self.op
    '''Chạy vòng lặp GS'''
    def cal_gaus(self):

        # lấy dữ liệu cần
        accur = self.ch_option()
        self.sbase = 100
        self.pgen = self._array2dict(self.gen_data['NO'], self.gen_data['P'])
        self.pload = self._array2dict(self.load_data['NO'], self.load_data['P'])
        self.qgen = self._array2dict(self.gen_data['NO'], self.gen_data['Q'])
        self.qload = self._array2dict(self.load_data['NO'], self.load_data['Q'])
        self.codebus = self._array2dict(self.bus_data['NO'], self.bus_data['Code'])
        self.vSched = self._array2dict(self.gen_data['NO'], self.gen_data['Vsched'])
        self.qmax = self._array2dict(self.gen_data['NO'], self.gen_data['Qmax'])
        self.qmin = self._array2dict(self.gen_data['NO'], self.gen_data['Qmin'])
        self.p = [0]*len(self.bus_data['NO'])
        self.q = [0]*len(self.bus_data['NO'])
        self.s = [0]*len(self.bus_data['NO'])

        # duyệt qua các Bus và tính công suất tại Bus
        for k in range(len(self.bus_data['NO'])):
            self.p[k] = ((self.pgen.get(k+1, 0) or 0) - (self.pload.get(k+1, 0)) or 0) / self.sbase
            self.q[k] = ((self.qgen.get(k+1, 0) or 0) - (self.qload.get(k+1, 0)) or 0) / self.sbase
            self.s[k] = complex(self.p[k], self.q[k])
        self.v =[0] * len(self.bus_data['NO'])

        # nếu code = [1,2,3] ---> [PQ bus, PV bus, Swing bus] ---> v
        for key, value in self.codebus.items():
            if value == 3:
                self.v[key -1] = self.vSched[key]
            elif value == 1:
                self.v[key -1] = 1.0
            elif value == 2:
                self.v[key -1] = self.vSched[key]

        # chạy vòng lặp
        k = 0
        while True:
            k += 1
            print(k)
            vn = self.gaus()
            dv = [vn[i] - self.v[i] for i in range(len(vn))]
            max_dv = max(abs(val) for val in dv)
            self.v = vn[:]
            if max_dv <= accur or k == 500:
                break
        self.k_dict = dict([('vòng lặp', k)])
        self.v = []
        self.v_delta = []
        for i in range(len(vn)):
            self.v.append(abs(vn[i]))
            self.v_delta.append(cmath.phase(vn[i]))
        self.vn_dict = self._array2dict(self.bus_data['NO'], vn)
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
            i = (self.vn_dict[self.frombus[key]] - self.vn_dict[self.tobus[key]])*self.ybus[value-1][self.tobus[key]-1]
            i_ = -i
            s = self.vn_dict[self.frombus[key]]*i.conjugate()
            s_ = self.vn_dict[self.tobus[key]]*i_.conjugate()
            sbr.append(s)
            sbr_.append(s_)
            ibr.append(i)
            ibr_.append(i_)
        self.sbr_dict = self._array2dict(self.line_data['NO'], sbr)
        self.sbr__dict = self._array2dict(self.line_data['NO'], sbr_)
        self.ibr_dict = self._array2dict(self.line_data['NO'], ibr )
        self.ibr__dict = self._array2dict(self.line_data['NO'], ibr_)
if __name__ == '__main__':
    dbfile = '5nut.db'
    op_idx = 'OP1'
    g = Gauss(dbfile, op_idx)
    g.ybus()
    g.cal_gaus()
    g.save_result()
    g.close_connection()
