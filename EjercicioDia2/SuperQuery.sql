

SELECT c.Name,100*sum(Winner*1.0)/Count(*) as WinRate,(100.0*COUNT(*)/(select count(*) from Stats AS PickRate)), AVG(kills*1.0) as AvgKills, AVG(Deaths*1.0) as AvgDeaths
FROM (select distinct * from [LoLDB].[dbo].[Stats]) as s
inner join (select distinct * from Participants) as p
on s.matchid = p.matchId and s.[participantid ] = p.ParticipantId
inner join Champions c on ChampionId = c.Id
group by c.Name
order by (100.0*COUNT(*))/(100.0*COUNT(*)/(select count(*) from Stats AS PickRate) desc
