package info.archinnov.achilles.type;

/**
 * Define naming strategy for keyspace name, table name and column names.Available values are:
 * <ul>
 *     <li>info.archinnov.achilles.type.NamingStrategy.SNAKE_CASE: transform all schema name using <a href="http://en.wikipedia.org/wiki/Snake_case" target="blank_">snake case</a></li>
 *     <li>info.archinnov.achilles.type.NamingStrategy.CASE_SENSITIVE: enclose the name between double quotes (") for escaping the case</li>
 *     <li>info.archinnov.achilles.type.NamingStrategy.LOWER_CASE: transform the name to lower case</li>
 * </ul>
 */
public enum NamingStrategy {

    /**
     * Convert Java field name into snake case convention <br/>
     *
     * Example:
     * <table style="border:1px solid">
     *      <thead>
     *          <tr>
     *              <th>Java name</th>
     *              <th>Camel case</th>
     *          </tr>
     *      </thead>
     *      <tbody>
     *          <tr>
     *              <td>count</td>
     *              <td>count</td>
     *          </tr>
     *          <tr>
     *              <td>column1</td>
     *              <td>column_1</td>
     *          </tr>
     *          <tr>
     *              <td>userName</td>
     *              <td>user_name</td>
     *          </tr>
     *          <tr>
     *              <td>FIRSTName</td>
     *              <td>FIRST_name</td>
     *          </tr>
     *      </tbody>
     * </table>
     */
    SNAKE_CASE,

    /**
     * Enclose Java field name with double quotes (") if the field has <strong>at least one</strong>upper-case character.
     * Otherwise let it as is. <br/>
     * Example:
     * <table style="border:1px solid">
     *      <thead>
     *          <tr>
     *              <th>Java name</th>
     *              <th>Case sensitive</th>
     *          </tr>
     *      </thead>
     *      <tbody>
     *          <tr>
     *              <td>count</td>
     *              <td>count</td>
     *          </tr>
     *          <tr>
     *              <td>column1</td>
     *              <td>column1</td>
     *          </tr>
     *          <tr>
     *              <td>userName</td>
     *              <td>"userName"</td>
     *          </tr>
     *          <tr>
     *              <td>FIRSTName</td>
     *              <td>"FIRSTName"</td>
     *          </tr>
     *      </tbody>
     * </table>
     */
    CASE_SENSITIVE,

    /**
     * Force Java field name to lower case.<br/>
     * Example:
     * <table style="border:1px solid">
     *      <thead>
     *          <tr>
     *              <th>Java name</th>
     *              <th>Lower case</th>
     *          </tr>
     *      </thead>
     *      <tbody>
     *          <tr>
     *              <td>count</td>
     *              <td>count</td>
     *          </tr>
     *          <tr>
     *              <td>column1</td>
     *              <td>column1</td>
     *          </tr>
     *          <tr>
     *              <td>userName</td>
     *              <td>username</td>
     *          </tr>
     *          <tr>
     *              <td>FIRSTName</td>
     *              <td>firstname</td>
     *          </tr>
     *      </tbody>
     * </table>
     */
    LOWER_CASE;
}
