from django.db import models

class players(models.Model):
	player = models.CharField(max_length = 100)
	position = models.CharField(max_length = 20)
	squad = models.CharField(max_length = 100)
	matches_played = models.IntegerField(default = 0)
	starts = models.IntegerField(default = 0)
	goals = models.IntegerField(default = 0)
	ast = models.IntegerField(default = 0)
	np_goals = models.IntegerField(default = 0)
	gls_and_ast = models.IntegerField(default = 0)

def __str__(self):
    return self.player


