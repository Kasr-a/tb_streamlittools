
def get_signal_strength_percentage(rssi, snr):
    if rssi and snr:
        if snr >= 7:
            if rssi > -40:
                return 100
            elif rssi < -120:
                return 60
            else:
                return (rssi * 0.5) + 120
        elif snr < 7 and snr > -10:
            return (snr * 3.529411765) + 35.29411765
        else:
            return 0

    else:
        return 0