<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:param name="name" />
  <xsl:param name="value" />

  <xsl:output indent="yes" />
  <xsl:preserve-space elements="*" />

  <xsl:template match="configuration">
    <configuration>
      <xsl:for-each select="./property">
        <xsl:choose>
           <xsl:when test="./name/text() = $name">
             <property>
               <name><xsl:value-of select="$name" /></name>
               <value><xsl:value-of select="$value" /></value>
               <description><xsl:value-of select="./description/text()" /></description>
             </property>
           </xsl:when>

           <xsl:otherwise>
             <xsl:copy-of select="." />
           </xsl:otherwise>
        </xsl:choose>
      </xsl:for-each>
    </configuration>
  </xsl:template>
</xsl:stylesheet>