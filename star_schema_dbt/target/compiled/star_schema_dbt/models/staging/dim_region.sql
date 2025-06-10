-- models/staging/dim_region.sql
-- Nouveau modèle pour la dimension région


SELECT
    DENSE_RANK() OVER (ORDER BY region_name) AS region_id,
    region_name,
    city
FROM (
    VALUES
        -- Mapping des villes vers les régions
        -- Tanger-Tétouan-Al Hoceima
        ('Al Aaroui', 'Tanger-Tétouan-Al Hoceima'),
        ('Fnideq', 'Tanger-Tétouan-Al Hoceima'),
        ('Chefchaouen', 'Tanger-Tétouan-Al Hoceima'),
        ('Assilah', 'Tanger-Tétouan-Al Hoceima'),
        ('Al Hoceima', 'Tanger-Tétouan-Al Hoceima'),
        ('Ajdir', 'Tanger-Tétouan-Al Hoceima'),
        
        -- Rabat-Salé-Kénitra
        ('Ain El Aouda', 'Rabat-Salé-Kénitra'),
        ('Rabat', 'Rabat-Salé-Kénitra'),
        ('Ain Attig', 'Rabat-Salé-Kénitra'),
        ('Harhoura', 'Rabat-Salé-Kénitra'),
        
        -- Grand Casablanca-Settat
        ('Casablanca', 'Grand Casablanca-Settat'),
        ('Dar Bouazza', 'Grand Casablanca-Settat'),
        ('Bouskoura', 'Grand Casablanca-Settat'),
        ('Ben Ahmed', 'Grand Casablanca-Settat'),
        ('Benslimane', 'Grand Casablanca-Settat'),
        ('Berrechid', 'Grand Casablanca-Settat'),
        ('El Borouj', 'Grand Casablanca-Settat'),
        ('El Jadida', 'Grand Casablanca-Settat'),
        ('Azemmour', 'Grand Casablanca-Settat'),
        ('Ain Harrouda', 'Grand Casablanca-Settat'),
        
        -- Marrakech-Safi
        ('Essaouira', 'Marrakech-Safi'),
        ('Asni', 'Marrakech-Safi'),
        ('Chichaoua', 'Marrakech-Safi'),
        ('Ben Guerir', 'Marrakech-Safi'),
        ('Ait Ourir', 'Marrakech-Safi'),
        ('El Kelaa Des Srarhna', 'Marrakech-Safi'),
        ('Imintanoute', 'Marrakech-Safi'),
        ('Amizmiz', 'Marrakech-Safi'),
        
        -- Fès-Meknès
        ('Fes', 'Fès-Meknès'),
        ('Ifrane', 'Fès-Meknès'),
        ('Azrou', 'Fès-Meknès'),
        ('Bhalil', 'Fès-Meknès'),
        ('El Hajeb', 'Fès-Meknès'),
        ('Boulemane', 'Fès-Meknès'),
        ('Imouzzer Kandar', 'Fès-Meknès'),
        ('Ain Leuh', 'Fès-Meknès'),
        ('Boufakrane', 'Fès-Meknès'),
        ('Ain Taoujdate', 'Fès-Meknès'),
        
        -- Béni Mellal-Khénifra
        ('Beni Mellal', 'Béni Mellal-Khénifra'),
        ('Azilal', 'Béni Mellal-Khénifra'),
        ('Demnate', 'Béni Mellal-Khénifra'),
        ('Fquih Ben Salah', 'Béni Mellal-Khénifra'),
        ('El Ksiba', 'Béni Mellal-Khénifra'),
        ('Bzou', 'Béni Mellal-Khénifra'),
        ('Aghbala', 'Béni Mellal-Khénifra'),
        ('Aguelmous', 'Béni Mellal-Khénifra'),
        
        -- Souss-Massa
        ('Agadir', 'Souss-Massa'),
        ('Aourir', 'Souss-Massa'),
        ('Inezgane', 'Souss-Massa'),
        ('Ait Melloul', 'Souss-Massa'),
        ('Biougra', 'Souss-Massa'),
        ('Aoulouz', 'Souss-Massa'),
        ('Imouzzer', 'Souss-Massa'),
        
        -- Oriental
        ('Berkane', 'Oriental'),
        ('Ahfir', 'Oriental'),
        ('Ain Aicha', 'Oriental'),
        ('Driouch', 'Oriental'),
        ('Guercif', 'Oriental'),
        ('Bouarfa', 'Oriental'),
        ('Figuig', 'Oriental'),
        ('Ain Bni Mathar', 'Oriental'),
        
        -- Drâa-Tafilalet
        ('Errachidia', 'Drâa-Tafilalet'),
        ('Er-Rich', 'Drâa-Tafilalet'),
        ('Goulmima', 'Drâa-Tafilalet'),
        ('Arfoud', 'Drâa-Tafilalet'),
        ('Er-Rissani', 'Drâa-Tafilalet'),
        ('Boumia', 'Drâa-Tafilalet'),
        ('Boumalne Dades', 'Drâa-Tafilalet'),
        ('Agdz', 'Drâa-Tafilalet'),
        
        -- Guelmim-Oued Noun
        ('Guelmim', 'Guelmim-Oued Noun'),
        ('Assa', 'Guelmim-Oued Noun'),
        ('Foum Zguid', 'Guelmim-Oued Noun'),
        
        -- Laâyoune-Sakia El Hamra
        ('Boujdour', 'Laâyoune-Sakia El Hamra'),
        
        -- Dakhla-Oued Ed-Dahab
        ('Dakhla', 'Dakhla-Oued Ed-Dahab'),
        
        -- Villes avec mapping approximatif (à vérifier selon vos données)
        ('Bradia', 'Fès-Meknès'),
        ('El Gara', 'Tanger-Tétouan-Al Hoceima'),
        ('Had Al Gharbia', 'Rabat-Salé-Kénitra'),
        ('Agourai', 'Fès-Meknès'),
        ('Bejaad', 'Grand Casablanca-Settat'),
        ('Houara Oulad Raho', 'Tanger-Tétouan-Al Hoceima'),
        ('El Guerdane', 'Souss-Massa'),
        ('Gueznaia', 'Tanger-Tétouan-Al Hoceima'),
        ('Ain Zohra', 'Rabat-Salé-Kénitra'),
        ('Ait M''Hamed', 'Béni Mellal-Khénifra'),
        ('Ait Erkha', 'Souss-Massa'),
        ('Issafen', 'Souss-Massa'),
        ('Irherm', 'Souss-Massa'),
        ('Belfaa', 'Souss-Massa'),
        ('Ait Ouassif', 'Souss-Massa'),
        ('Ait Baha', 'Souss-Massa'),
        ('Tafraout', 'Souss-Massa'),
        ('Taliouine', 'Souss-Massa'),
        ('Tiznit', 'Souss-Massa'),
        ('Tafoughalt', 'Oriental'),
        ('Bni Oukil', 'Fès-Meknès'),
        ('Bni Bouayach', 'Tanger-Tétouan-Al Hoceima'),
        ('Bni Yakhlef', 'Grand Casablanca-Settat'),
        ('Bni Drar', 'Oriental'),
        ('Bni Hadifa', 'Tanger-Tétouan-Al Hoceima'),
        ('Bab Taza', 'Fès-Meknès'),
        ('Bab Marzouka', 'Fès-Meknès'),
        ('El Kbab', 'Fès-Meknès'),
        ('Bni Chiker', 'Tanger-Tétouan-Al Hoceima'),
        ('Dcheira El Jihadia', 'Souss-Massa'),
        ('Issaguen', 'Souss-Massa'),
        ('Ghmate', 'Marrakech-Safi'),
        ('Imzouren', 'Tanger-Tétouan-Al Hoceima'),
        ('Chougrane', 'Fès-Meknès'),
        ('Bni Yakhlef', 'Grand Casablanca-Settat'),
        ('Arbaa Rasmouka', 'Marrakech-Safi'),
        ('El Mansouria', 'Grand Casablanca-Settat'),
        ('El Aioun Sidi Mellouk', 'Oriental'),
        ('Bni Hadifa', 'Tanger-Tétouan-Al Hoceima'),
        ('Bab Taza', 'Fès-Meknès'),
        ('El Kbab', 'Fès-Meknès'),
        ('Fdalate', 'Marrakech-Safi'),
        ('Bouguedra', 'Grand Casablanca-Settat'),
        ('Arbaoua', 'Rabat-Salé-Kénitra'),
        ('Dar Chaoui', 'Rabat-Salé-Kénitra'),
        ('Adrej', 'Souss-Massa'),
        ('Bni Drar', 'Oriental'),
        ('Brachoua', 'Grand Casablanca-Settat'),
        ('Irigh N''Tahala', 'Souss-Massa'),
        ('Ghafsai', 'Fès-Meknès'),
        ('Ezzhiliga', 'Souss-Massa'),
        ('Bab Marzouka', 'Fès-Meknès'),
        ('Bni Bouayach', 'Tanger-Tétouan-Al Hoceima'),
        ('Echemmaia', 'Fès-Meknès'),
        ('Dar Ould Zidouh', 'Marrakech-Safi'),
        ('Boudinar', 'Tanger-Tétouan-Al Hoceima'),
        ('Ain Dfali', 'Fès-Meknès'),
        ('Ait Ishaq', 'Marrakech-Safi'),
        ('Boujniba', 'Grand Casablanca-Settat'),
        ('Ain Cheggag', 'Fès-Meknès'),
        ('Had Dra', 'Marrakech-Safi'),
        ('Dar El Kebdani', 'Oriental'),
        
        -- Région par défaut pour les villes non mappées
        ('Autre', 'Non définie')
) AS regions_mapping(city, region_name)