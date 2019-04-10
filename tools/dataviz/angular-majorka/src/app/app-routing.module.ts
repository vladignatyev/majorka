import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { CampaignsComponent } from './campaigns/campaigns.component';
import { DashboardComponent } from './dashboard/dashboard.component';


const routes: Routes = [
  { path: '/campaigns', component: CampaignsComponent },
  { path: '/dashboard', component: DashboardComponent },
  { path: '', redirectTo: '/dashboard', pathMatch: null }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
