import matplotlib.pyplot as plt


plt.plot([i * 0.1 - 5 for i in range(100)],
         [(i * 0.1 - 5) ** 2 for i in range(100)])
plt.show()
