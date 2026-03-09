import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QLabel

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('Main Window')
        self.setGeometry(100, 100, 600, 400)

        label = QLabel('Hello, PyQt5!', self)
        label.setGeometry(250, 180, 200, 40)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())