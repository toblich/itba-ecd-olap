import datetime


class ProgressBar:

    def __init__(self, total_items, action='', bar_len=100):
        self.action = action
        self.bar_len = bar_len
        self.total_items = total_items
        self.current_step = 0
        self.current_perc = 0
        self.last_round_perc = 0
        self.prev_stamp = None
        self.time_steps = []

    def tick(self):
        # First empty bar
        if self.current_step == 0:
            self.print_progress()

        self.current_step += 1
        self.current_perc = self.current_step / self.total_items * 100
        if (round(self.current_perc) > self.last_round_perc) and (self.current_perc <= 100) and self.current_step > 1:
            self.last_round_perc = round(self.current_perc)
            self.print_progress()

        self.prev_stamp = datetime.datetime.now()

    def print_progress(self):
        remaining_time = "Inf"
        if self.current_step > 0:
            time_step = datetime.datetime.now() - self.prev_stamp
            self.time_steps.append(time_step)
            avg_time_step = sum(self.time_steps, datetime.timedelta()) / len(self.time_steps)
            remaining_time = avg_time_step * (self.total_items - self.current_step)

        filled_len = int(round(self.bar_len * self.last_round_perc / 100.0))

        bar = '=' * filled_len + '-' * (self.bar_len - filled_len)
        status = f" - COMPLETE {' ' * 20}" if self.last_round_perc >= 100 else f' - Remaining: {str(remaining_time)}'
        end_char = '' if self.last_round_perc < 100 else '\n'  # On 100% we need a new line, otherwise a carriage return
        print("\r", end='')
        print(f'{self.action} [{bar}] {self.last_round_perc}%{status}', end=end_char)
