import React from 'react';
import { Link } from 'react-router-dom';

/**
 * Presents a simple tabular list of alojamientos with actions to
 * abrir el calendario o editar cada registro.  The properties
 * prop must be an array of objects with an `id` and the usual
 * property fields.  If you need deletion, you can extend this
 * table with an onDelete callback.
 */
const PropertyTable = ({ properties }) => (
  <table className="table">
    <thead>
      <tr>
        <th>ID</th>
        <th>Nombre</th>
        <th>Ciudad</th>
        <th>Barrio</th>
        <th>Tipo</th>
        <th>Habitaciones</th>
        <th>Capacidad</th>
        <th>Precio Medio</th>
        <th>Acciones</th>
      </tr>
    </thead>
    <tbody>
      {properties.map((p) => (
        <tr key={p.id}>
          <td>{p.id}</td>
          <td>{p.name}</td>
          <td>{p.city}</td>
          <td>{p.zone}</td>
          <td>{p.type}</td>
          <td>{p.rooms}</td>
          <td>{p.capacity}</td>
          <td>{p.priceMedian}</td>
          <td>
            <Link to={`/properties/${p.id}/calendar`}>Calendario</Link>{' '}
            |
            {' '}
            <Link to={`/properties/${p.id}/edit`}>Editar</Link>
          </td>
        </tr>
      ))}
    </tbody>
  </table>
);

export default PropertyTable;
